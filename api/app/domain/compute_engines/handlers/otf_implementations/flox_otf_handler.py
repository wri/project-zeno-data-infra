from functools import partial

import numpy as np
import pandas as pd
from app.domain.compute_engines.handlers.analytics_otf_handler import (
    AnalyticsOTFHandler,
)
from app.domain.models.dataset import Dataset, DatasetQuery
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from flox.xarray import xarray_reduce


class FloxOTFHandler(AnalyticsOTFHandler):
    EXPECTED_GROUPS = {
        Dataset.tree_cover_loss: np.arange(0, 25),
        # TODO add Dataset.tree_cover_gain
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

        results["aoi_type"] = aoi_type
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

        results["aoi_id"] = aoi_id
        filtered_results = results[
            ~np.isnan(results[query.aggregate.dataset.get_field_name()])
        ]

        if query.aggregate.dataset == Dataset.area_hectares:
            filtered_results[
                query.aggregate.dataset.get_field_name()
            ] = filtered_results[query.aggregate.dataset.get_field_name()]

        # TODO remove band and spatial ref from zarrs
        return filtered_results.reset_index().drop(
            columns=["index", "band", "spatial_ref"], errors="ignore"
        )
