import os
from functools import partial

import duckdb
import numpy as np
import pandas as pd
import s3fs
from app.domain.models.dataset import Dataset, DatasetQuery
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.base import StrictBaseModel
from flox.xarray import xarray_reduce


class DuckDbPrecalcQueryService(StrictBaseModel):
    def __init__(self):
        pass

    async def execute(self, table_uri: str, query: str) -> pd.DataFrame:
        duckdb.query(
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain,
                CHAIN config
            );
        """
        )

        fs = s3fs.S3FileSystem(requester_pays=True)
        table = os.path.splitext(os.path.basename(table_uri))[0]
        fs.get(
            table_uri,
            f"/tmp/{table}",
        )

        # need to declare this to bind FROM in SQL query
        data_source = duckdb.read_parquet(f"/tmp/{table}")

        # TODO duckdb has no native async, need to use aioduckdb? Check if blocking in load test
        df = duckdb.sql(query).df()
        return df.to_dict(orient="list")


class PrecalcHandler:
    FIELDS = {
        Dataset.area_hectares: "value",
        Dataset.tree_cover_loss: "loss_year",
        Dataset.canopy_cover: "canopy_cover",
    }

    def __init__(self, precalc_query_service, next_handler):
        self.precalc_query_service = precalc_query_service
        self.next_handler = next_handler

    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        if (
            aoi_type == "admin"
            and query.aggregate.dataset == Dataset.area_hectares
            and query.group_bys == [Dataset.tree_cover_loss]
        ):
            data_source = "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/tabular/zonal_stats/umd_tree_cover_loss_by_driver.parquet"
        else:
            return await self.next_handler.handle(aoi_type, aoi_ids, query)

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
        aoi_geometries = await self.aoi_geometry_repository.load(aoi_type, aoi_ids)
        aoi_partial = partial(
            self._handle,
            query=query,
            dataset_repository=self.dataset_repository,
            expected_groups_per_dataset=self.EXPECTED_GROUPS,
        )
        futures = self.dask_client.map(aoi_partial, list(zip(aoi_ids, aoi_geometries)))
        results_per_aoi = await self.dask_client.gather(futures)

        results = pd.concat(results_per_aoi)

        for dataset in query.group_bys:
            col = dataset.get_field_name()
            results[col] = self.dataset_repository.unpack(dataset, results[col])

        return results.to_dict(orient="list")

    @staticmethod
    def _handle(aoi, query, dataset_repository, expected_groups_per_dataset):
        aoi_id, aoi_geometry = aoi
        by = dataset_repository.load(query.aggregate.dataset, geometry=aoi_geometry)
        func = query.aggregate.func

        objs = []
        expected_groups = []
        for filter in query.filters:
            translated_value = dataset_repository.translate(
                filter.dataset, filter.value
            )
            da = dataset_repository.load(filter.dataset, geometry=aoi_geometry)
            by = by.where(eval(f"da {filter.op} {translated_value}"))

            if filter.dataset in query.group_bys:
                # filter expected groups by the filter itself so it doesn't appear in the results as 0s
                expected_groups_per_dataset[
                    filter.dataset
                ] = expected_groups_per_dataset[filter.dataset][
                    eval(
                        f"expected_groups_per_dataset[{filter.dataset}] {filter.op} {translated_value}"
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

        results["id"] = aoi_id
        filtered_results = results[
            ~np.isnan(results[query.aggregate.dataset.get_field_name()])
        ]
        return filtered_results.reset_index().drop(
            columns=["index", "band", "spatial_ref"]
        )


class ComputeEngine:
    def __init__(self, handler):
        self.handler = handler

    async def compute(self, aoi_type, aoi_ids, query: DatasetQuery):
        return await self.handler.handle(aoi_type, aoi_ids, query)
