from __future__ import annotations

from functools import partial
from typing import Dict, List, Sequence, Tuple

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
    """
    Key change vs previous version:

    - We DO NOT submit `_handle` to the cluster (no client.map(_handle,...)).
      `_build_lazy_result` runs on the client and returns *lazy* xarray objects
      (dask graphs).
    - We then submit those lazy objects with `client.compute(...)` and gather.
    - Only after gather do we convert to pandas and post-process.

    This avoids the "one worker spending forever in handle creating tasks" pattern.
    """

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

        # Build lazy results (dask graphs) LOCALLY.
        build_partial = partial(
            self._build_lazy_result,
            query=query,
            dataset_repository=self.dataset_repository,
            expected_groups_per_dataset=self.EXPECTED_GROUPS,
        )
        lazy_results: List[xr.Dataset] = [
            build_partial((aoi_id, geom))
            for aoi_id, geom in zip(aoi.ids, aoi_geometries)
        ]

        # Submit all graphs to the cluster.
        futures = self.dask_client.compute(lazy_results)
        computed: List[xr.Dataset] = await self.dask_client.gather(futures)

        # Convert results to pandas after compute completes.
        results_per_aoi: List[pd.DataFrame] = [
            self._xr_result_to_df(xr_obj, query=query) for xr_obj in computed
        ]
        results = pd.concat(results_per_aoi, ignore_index=True)

        # Unpack group-by coded values (same as before)
        for dataset in query.group_bys:
            col = dataset.get_field_name()
            if col in results.columns:
                results[col] = self.dataset_repository.unpack(dataset, results[col])

        results["aoi_type"] = aoi.type
        return results.to_dict(orient="list")

    # ---------------------------------------------------------------------
    # Lazy graph builder (runs on client)
    # ---------------------------------------------------------------------

    @staticmethod
    def _build_lazy_result(aoi, query, dataset_repository, expected_groups_per_dataset):
        """
        Build and return a *lazy* xarray.Dataset (dask-backed), with:
        - group-by dimensions (if any)
        - aggregate variables
        - an `aoi_id` coord

        IMPORTANT:
        - No `.compute()`
        - No `.to_dataframe()`
        """
        aoi_id, aoi_geometry = aoi
        func = query.aggregate.func

        # Copy expected groups per AOI because filters may shrink them.
        # NOTE: values are numpy arrays; copy them so we don't mutate shared state.
        expected_groups_local: Dict[Dataset, np.ndarray] = {
            k: np.array(v, copy=True) for k, v in expected_groups_per_dataset.items()
        }

        by = xr.Dataset()
        for ds in query.aggregate.datasets:
            xarr = dataset_repository.load(ds, geometry=aoi_geometry).reindex_like(
                by, method="nearest", tolerance=1e-5
            )
            by[ds.get_field_name()] = xarr

        # AOI mask (lazy via map_blocks)
        geom_mask = FloxOTFHandler._lazy_geometry_mask_from_da(
            next(iter(by.data_vars.values())), aoi_geometry
        )
        by = by.where(geom_mask, other=np.nan)

        # Apply filters (still lazy)
        for flt in query.filters:
            translated_value = dataset_repository.translate(flt.dataset, flt.value)
            arr = dataset_repository.load(flt.dataset, geometry=aoi_geometry)
            filter_arr = FloxOTFHandler._get_filter_by_op(arr, flt.op, translated_value)
            by = by.where(filter_arr)

            # If a filter applies to a group-by dataset, shrink expected_groups so
            # flox doesn't emit all other groups as zeros/NaNs.
            if flt.dataset in query.group_bys:
                expected_groups_local[flt.dataset] = expected_groups_local[flt.dataset][
                    FloxOTFHandler._get_filter_by_op(
                        expected_groups_local[flt.dataset],
                        flt.op,
                        translated_value,
                    )
                ]

        # Group-by arrays + expected groups
        objs: List[xr.DataArray] = []
        expected_groups: List[np.ndarray] = []

        for group_by in query.group_bys:
            garr = dataset_repository.load(
                group_by, geometry=aoi_geometry
            ).reindex_like(by, method="nearest", tolerance=1e-5)
            objs.append(garr)
            expected_groups.append(expected_groups_local[group_by])

        # Compute lazily
        if len(objs) > 0:
            reduced = xarray_reduce(
                by,
                *objs,
                func=func,
                fill_value=np.nan,
                min_count=1,
                expected_groups=tuple(expected_groups),
            )
        else:
            # Keep result lazy (no compute here)
            if func == "sum":
                reduced = by.sum()
            elif func == "count":
                reduced = by.count()
            else:
                raise ValueError(f"{func} unsupported.")

        # Attach AOI id as a coord so it survives compute and conversion
        reduced = reduced.assign_coords(aoi_id=aoi_id)
        return reduced

    # ---------------------------------------------------------------------
    # Post-compute conversion (runs on client after gather)
    # ---------------------------------------------------------------------

    @staticmethod
    def _xr_result_to_df(xr_obj: xr.Dataset, query: DatasetQuery) -> pd.DataFrame:
        """
        Convert computed xarray result to a DataFrame and apply the same filtering
        you had before (drop rows where all aggregate columns are NaN).
        """
        df = xr_obj.to_dataframe().reset_index()

        # Ensure aoi_id is a column
        if "aoi_id" not in df.columns:
            if "aoi_id" in xr_obj.coords:
                df["aoi_id"] = xr_obj.coords["aoi_id"].item()
            else:
                df["aoi_id"] = None

        agg_col_names = [ds.get_field_name() for ds in query.aggregate.datasets]
        existing_agg_cols = [c for c in agg_col_names if c in df.columns]
        if existing_agg_cols:
            df = df[~df[existing_agg_cols].isna().all(axis=1)]

        # TODO remove band and spatial_ref from zarrs
        df = df.drop(columns=["band", "spatial_ref"], errors="ignore")
        # Don't keep the auto index columns from xarray/pandas conversions
        df = df.reset_index(drop=True)
        return df

    # ---------------------------------------------------------------------
    # Utility ops (mostly unchanged)
    # ---------------------------------------------------------------------

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
        raise ValueError(f"Unsupported op: {op}")

    @staticmethod
    def _get_expected_group_filter_by_op(expected_group, op, value):
        # (kept for compatibility; not used directly in this refactor)
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
        raise ValueError(f"Unsupported op: {op}")

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

        dx = float(x[1] - x[0])
        dy = float(y[1] - y[0])

        x0 = float(x[0] - dx / 2.0)
        y0 = float(y[0] - dy / 2.0)
        base_transform = Affine.translation(x0, y0) * Affine.scale(dx, dy)

        geom_json = mapping(geom)

        y_chunks = template.data.chunks[0]
        x_chunks = template.data.chunks[1]

        y_offsets = np.cumsum((0,) + y_chunks[:-1])
        x_offsets = np.cumsum((0,) + x_chunks[:-1])

        def _mask_block(block, block_info=None, block_id=None):
            if block.size == 0:
                return np.zeros(block.shape, dtype=bool)

            if block_id is not None:
                by, bx = block_id
            else:
                if not block_info:
                    return np.zeros(block.shape, dtype=bool)
                info = next(iter(block_info.values()))
                by, bx = info.get("chunk-location", (0, 0))

            y0i = int(y_offsets[by])
            x0i = int(x_offsets[bx])

            chunk_transform = base_transform * Affine.translation(x0i, y0i)

            return geometry_mask(
                [geom_json],
                out_shape=block.shape,
                transform=chunk_transform,
                invert=True,
                all_touched=all_touched,
            )

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
