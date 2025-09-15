from functools import partial

import numpy as np
import pandas as pd
import xarray as xr
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
        Dataset.tree_cover_gain: np.arange(0, 5),
        Dataset.canopy_cover: np.arange(0, 8),
        Dataset.tree_cover_loss_drivers: np.arange(0, 8),
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
        func = query.aggregate.func

        by = xr.Dataset()
        for ds in query.aggregate.datasets:
            xarr = dataset_repository.load(ds, geometry=aoi_geometry).reindex_like(
                by, method="nearest", tolerance=1e-5
            )
            by[ds.get_field_name()] = xarr

        objs = []
        expected_groups = []
        for filter in query.filters:
            translated_value = dataset_repository.translate(
                filter.dataset, filter.value
            )
            da = dataset_repository.load(filter.dataset, geometry=aoi_geometry)
            filter_arr = FloxOTFHandler._get_filter_by_op(
                da, filter.op, translated_value
            )
            by = by.where(filter_arr)

            if filter.dataset in query.group_bys:
                # filter expected groups by the filter itself so it doesn't appear in the results as 0s
                expected_groups_per_dataset[
                    filter.dataset
                ] = expected_groups_per_dataset[filter.dataset][
                    FloxOTFHandler._get_filter_by_op(
                        expected_groups_per_dataset[filter.dataset],
                        filter.op,
                        translated_value,
                    )
                ]

        for group_by in query.group_bys:
            da = dataset_repository.load(group_by, geometry=aoi_geometry).reindex_like(
                by, method="nearest", tolerance=1e-5
            )
            objs.append(da)
            expected_groups.append(expected_groups_per_dataset[group_by])

            results = (
                xarray_reduce(
                    by, *objs, func=func, expected_groups=tuple(expected_groups)
                )
                .to_dataframe()
                .reset_index()
            )
        else:
            results = FloxOTFHandler._apply_xarr_func(by, func)

        # Filter out rows where results for all aggregate datasets are NaN
        results["aoi_id"] = aoi_id
        agg_col_names = [ds.get_field_name() for ds in query.aggregate.datasets]
        filtered_results = results[~results[agg_col_names].isna().all(axis=1)]

        # TODO remove band, spatial_ref, x, y from zarrs
        return filtered_results.reset_index().drop(
            columns=["index", "band", "spatial_ref"], errors="ignore"
        )

    @staticmethod
    def _apply_xarr_func(by, func):
        if func == "sum":
            scalar = by.sum().compute()
        elif func == "count":
            scalar = by.count().compute()
        else:
            raise ValueError(f"{func} unsupported.")

        # to convert scalar to dataframe, need to do some pandas index gymnastics
        results = (
            scalar.expand_dims(dim=["index"])
            .to_dataframe()
            .reset_index()
            .drop(columns=["index"])
        )

        return results

    @staticmethod
    def _get_filter_by_op(arr, op, value):
        match op:
            case ">":
                return arr > value
            case "<":
                return arr < value
            case ">=":
                return arr >= value
            case "<=":
                return arr <= value
            case "=":
                return arr == value
            case "!=":
                return arr != value
            case "in":
                if isinstance(arr, xr.DataArray) or isinstance(arr, xr.Dataset):
                    return arr.isin(value)
                elif isinstance(arr, np.ndarray):
                    return np.isin(arr, value)

    @staticmethod
    def _get_expected_group_filter_by_op(expected_group, op, value):
        match op:
            case ">":
                return expected_group > value
            case "<":
                return expected_group < value
            case ">=":
                return expected_group >= value
            case "<=":
                return expected_group <= value
            case "=":
                return expected_group == value
            case "!=":
                return expected_group != value
            case "in":
                return set(expected_group) & set(value)
