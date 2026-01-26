from functools import partial

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
from affine import Affine
from flox.xarray import xarray_reduce
from rasterio.features import geometry_mask
from shapely.geometry import mapping, shape

from app.domain.compute_engines.handlers.analytics_otf_handler import (
    AnalyticsOTFHandler,
)
from app.domain.models.dataset import Dataset, DatasetQuery
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository


class FloxOTFHandler(AnalyticsOTFHandler):
    EXPECTED_GROUPS = {
        Dataset.tree_cover_loss: np.arange(0, 25),
        Dataset.tree_cover_gain: np.arange(0, 5),
        Dataset.canopy_cover: np.arange(0, 8),
        Dataset.tree_cover_loss_drivers: np.arange(0, 8),
        Dataset.natural_forests: np.arange(0, 3),
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

    async def handle(self, aoi, query: DatasetQuery):
        if aoi.type == "feature_collection":
            aoi_geometries = [
                shape(feature) for feature in aoi.feature_collection["features"]
            ]
        else:
            aoi_geometries = await self.aoi_geometry_repository.load(aoi.type, aoi.ids)

        aoi_partial = partial(
            self._handle,
            query=query,
            dataset_repository=self.dataset_repository,
            expected_groups_per_dataset=self.EXPECTED_GROUPS,
        )
        futures = self.dask_client.map(aoi_partial, list(zip(aoi.ids, aoi_geometries)))
        results_per_aoi = await self.dask_client.gather(futures)

        results = pd.concat(results_per_aoi)

        for dataset in query.group_bys:
            col = dataset.get_field_name()
            results[col] = self.dataset_repository.unpack(dataset, results[col])

        results["aoi_type"] = aoi.type
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

        geometry_mask = FloxOTFHandler._lazy_geometry_mask_from_da(
            next(iter(by.data_vars.values())), aoi_geometry
        )
        by = by.where(geometry_mask, other=np.nan)

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

        if len(objs) > 0:
            results = (
                xarray_reduce(
                    by,
                    *objs,
                    func=func,
                    fill_value=np.nan,
                    min_count=1,
                    expected_groups=tuple(expected_groups),
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

        # TODO remove band and spatial_ref from zarrs
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

    @staticmethod
    def _lazy_geometry_mask_from_da(
        template: xr.DataArray,
        geom,
        *,
        all_touched: bool = False,
    ) -> xr.DataArray:
        """
        Lazily rasterize `geom` onto `template`'s grid using dask.map_blocks.

        Assumptions:
        - template is 2D with dims ('y','x') (will transpose if needed)
        - template has 1D coords 'x' and 'y' representing pixel centers
        - rectilinear grid (constant dx/dy)

        Returns:
        xr.DataArray of dtype bool with same shape/chunks as template,
        True inside geom, False outside.
        """
        if not hasattr(template.data, "chunks"):
            raise ValueError(
                "template must be chunked (dask-backed). Use template.chunk(...) first."
            )

        x = template["x"].values
        y = template["y"].values

        # Pixel size (y often descending -> dy negative; that's OK)
        dx = float(x[1] - x[0])
        dy = float(y[1] - y[0])

        # Rasterio expects transform for *pixel corner* of the upper-left pixel.
        x0 = float(x[0] - dx / 2.0)
        y0 = float(y[0] - dy / 2.0)
        base_transform = Affine.translation(x0, y0) * Affine.scale(dx, dy)

        geom_json = mapping(geom)

        # chunks are per-dimension sizes, e.g. ((5,5),(5,5))
        y_chunks = template.data.chunks[0]
        x_chunks = template.data.chunks[1]

        # cumulative pixel offsets for each block index
        y_offsets = np.cumsum((0,) + y_chunks[:-1])
        x_offsets = np.cumsum((0,) + x_chunks[:-1])

        def _mask_block(block, block_info=None, block_id=None):
            # Dask may call once for dtype/shape inference with an empty (0,0) block.
            # Just return an empty boolean array in that case.
            if block.size == 0:
                return np.zeros(block.shape, dtype=bool)

            # Prefer block_id if provided (most reliable)
            if block_id is not None:
                by, bx = block_id
            else:
                # Fallback: pull chunk-location out of block_info
                if not block_info:
                    return np.zeros(block.shape, dtype=bool)
                info = next(iter(block_info.values()))
                by, bx = info.get("chunk-location", (0, 0))

            y0i = int(y_offsets[by])
            x0i = int(x_offsets[bx])

            chunk_transform = base_transform * Affine.translation(x0i, y0i)

            return geometry_mask(
                [geom_json],
                out_shape=block.shape,  # (chunk_y, chunk_x)
                transform=chunk_transform,
                invert=True,
                all_touched=all_touched,
            )

        # Dummy array purely to drive chunking & block_info
        driver = da.zeros(
            template.shape,
            chunks=template.data.chunks,
            dtype=np.uint8,
        )

        mask = da.map_blocks(
            _mask_block,
            driver,
            dtype=bool,
            chunks=template.data.chunks,
        )

        return xr.DataArray(mask, coords=template.coords, dims=("y", "x"))
