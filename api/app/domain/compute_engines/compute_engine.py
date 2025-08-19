from functools import partial
from typing import Any, List, Literal

import duckdb
import numpy as np
import pandas as pd
from app.domain.models.dataset import Dataset
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.areas_of_interest import AreaOfInterest
from app.models.common.base import StrictBaseModel
from flox.xarray import xarray_reduce


class DatasetFilter(StrictBaseModel):
    dataset: Dataset
    op: Literal["=", "<", ">", "<=", ">=", "!="]
    value: Any


class DatasetAggregate(StrictBaseModel):
    dataset: Dataset
    func: Literal["sum", "count"]


class DatasetQuery(StrictBaseModel):
    aggregate: DatasetAggregate
    group_bys: List[Dataset]
    filters: List[DatasetFilter]


class PrecalcQueryService(StrictBaseModel):
    def __init__(self):
        pass

    async def execute(self, data_source: str, query: str) -> pd.DataFrame:
        return await duckdb.sql(query).df()


class PrecalcHandler:
    FIELDS = {
        Dataset.area_hectares: "area_ha",
        Dataset.tree_cover_loss: "loss_year",
        Dataset.canopy_cover: "canopy_cover",
    }

    def __init__(self, precalc_query_service):
        self.precalc_query_service = precalc_query_service

    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        if query.aggregate.dataset == Dataset.area_hectares and query.group_bys == [
            Dataset.tree_cover_loss
        ]:
            data_source = "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/tabular/zonal_stats/umd_tree_cover_loss_by_driver.parquet"

        agg = f"{query.aggregate.func.upper()}({self.FIELDS[query.aggregate.dataset]}) AS {self.FIELDS[query.aggregate.dataset]}"
        groupby_fields = ", ".join(
            [self.FIELDS[dataset] for dataset in query.group_bys]
        )
        filters = " AND ".join(
            [
                f"{self.FIELDS[filt.dataset]} {filt.op} {str(filt.value)}"
                for filt in query.filters
            ]
        )
        filters += f" AND id in {tuple(aoi_ids)}"
        sql = f"SELECT id, {groupby_fields}, {agg} FROM data_source WHERE {filters} GROUP BY id, {groupby_fields}"

        return await self.precalc_query_service.execute(data_source, sql)


class FloxOTFHandler:
    EXPECTED_GROUPS = {
        Dataset.tree_cover_loss: np.arange(0, 25),
        Dataset.canopy_cover: np.arange(0, 8),
    }

    def __init__(
        self,
        dataset_repository=ZarrDatasetRepository(),
        aoi_geometry_repository=DataApiAoiGeometryRepository(),
        dask_client=None,
    ):
        self.dataset_repository = dataset_repository
        self.aoi_geometry_repository = aoi_geometry_repository
        self.dask_client = dask_client

    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        aoi_geometries = self.aoi_geometry_repository.load(aoi_type, aoi_ids)
        aoi_partial = partial(
            self._handle,
            query=query,
            dataset_repository=self.dataset_repository,
            expected_groups_per_dataset=self.EXPECTED_GROUPS,
        )
        futures = self.dask_client.map(aoi_partial, aoi_geometries)
        results_per_aoi = await self.dask_client.gather(futures)

        results = pd.concat(results_per_aoi)
        return results

    @staticmethod
    async def _handle(
        aoi_geometry, query, dataset_repository, expected_groups_per_dataset
    ):
        by = dataset_repository.load(query.aggregate.dataset, geometry=aoi_geometry)
        func = query.aggregate.func

        objs = []
        expected_groups = []
        for filter in query.filters:
            da = dataset_repository.load(filter.dataset, geometry=aoi_geometry)
            by = by.where(eval(f"da {filter.op} {filter.value}"))

            if filter.dataset in query.group_bys:
                # filter expected groups by the filter itself so it doesn't appear in the results as 0s
                expected_groups_per_dataset[
                    filter.dataset
                ] = expected_groups_per_dataset[filter.dataset][
                    eval(
                        f"expected_groups_per_dataset[{filter.dataset}] {filter.op} {filter.value}"
                    )
                ]

        for group_by in query.group_bys:
            da = dataset_repository.load(group_by, geometry=aoi_geometry)
            objs.append(da)
            expected_groups.append(expected_groups_per_dataset[group_by])

        results = (
            xarray_reduce(by, *objs, func=func, expected_groups=tuple(expected_groups))
            .to_dataframe()
            .reset_index()
        )
        filtered_results = results[
            ~np.isnan(results[query.aggregate.dataset.get_field_name()])
        ]
        return filtered_results.reset_index().drop(columns="index")


class ComputeEngine:
    def __init__(self, handler):
        self.handler = handler

    async def compute(self, aoi_type, aoi_ids, query: DatasetQuery):
        return await self.handler.handle(aoi_type, aoi_ids, query)


class AreaOfInterestList:
    def __init__(self, aois: AreaOfInterest, compute_engine: ComputeEngine):
        self.type = aois.type
        self.ids = aois.ids
        self.compute_engine = compute_engine

    async def get_tree_cover_loss(
        self, canopy_cover: int, start_year: int, end_year: int, forest_type: str
    ):
        query = DatasetQuery(
            aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
            group_bys=[Dataset.tree_cover_loss],
            filters=[
                DatasetFilter(
                    dataset=Dataset.canopy_cover, op=">=", value=canopy_cover
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op=">=",
                    value=start_year,
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op="<=",
                    value=end_year,
                ),
            ],
        )
        return await self.compute_engine.compute(self.type, self.ids, query)
