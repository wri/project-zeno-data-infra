import dask.array as da
import numpy as np
import rioxarray  # noqa: F401 — needed for .rio accessor
import xarray as xr
from shapely.geometry import Polygon, box

from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository


def _make_large_dask_dataarray(
    nx: int = 1200,
    ny: int = 1200,
    fill_value: int = 1,
    nodata: float = 0,
    chunk_size: int = 400,
) -> xr.DataArray:
    """Create a dask-backed DataArray with `nx` x `ny` pixels covering [0, nx)×[0, ny).

    Resolution is 1 unit per pixel.  Coordinates are arranged so that
    x increases left→right and y decreases top→bottom (standard geo order).
    """
    data = da.full(
        (ny, nx), fill_value, dtype=np.int32, chunks=(chunk_size, chunk_size)
    )
    x_coords = np.linspace(0.5, nx - 0.5, nx)  # pixel-centre coordinates
    y_coords = np.linspace(ny - 0.5, 0.5, ny)  # descending
    arr = xr.DataArray(data, dims=("y", "x"), coords={"y": y_coords, "x": x_coords})
    arr.rio.write_crs("EPSG:4326", inplace=True)
    arr.rio.write_nodata(nodata, inplace=True)
    arr.name = "test_band"
    return arr


class TestClipXarrToGeometryLazyPath:
    """Tests that exercise the lazy da.map_blocks clipping path
    (arrays with >= 1000 pixels on both axes)."""

    repo = ZarrDatasetRepository()

    # --- basic masking ---------------------------------------------------

    def test_lazy_clip_masks_outside_geometry(self):
        """Pixels outside the clip geometry should be set to nodata."""
        arr = _make_large_dask_dataarray(nx=1200, ny=1200, fill_value=5, nodata=0)

        # Use a diamond so corners of its bounding box fall outside the geometry
        clip_geom = Polygon([(600, 300), (900, 600), (600, 900), (300, 600)])
        result = self.repo._clip_xarr_to_geometry(arr, clip_geom)

        result_np = result.values  # compute

        # Pixels fully inside the diamond should keep their original value
        y_centre = int(np.argmin(np.abs(result.y.values - 600)))
        x_centre = int(np.argmin(np.abs(result.x.values - 600)))
        assert result_np[y_centre, x_centre] == 5

        # A corner of the bounding box (inside the bbox but outside the
        # diamond) should be masked to nodata (0)
        y_corner = int(np.argmin(np.abs(result.y.values - 300.5)))
        x_corner = int(np.argmin(np.abs(result.x.values - 300.5)))
        assert (
            result_np[y_corner, x_corner] == 0
        ), f"Corner pixel should be nodata (0) but got {result_np[y_corner, x_corner]}"

    def test_lazy_clip_preserves_dtype(self):
        """The returned array dtype must match the original."""
        arr = _make_large_dask_dataarray(nx=1200, ny=1200, fill_value=7, nodata=0)
        clip_geom = box(100, 100, 1100, 1100)
        result = self.repo._clip_xarr_to_geometry(arr, clip_geom)
        assert result.dtype == arr.dtype

    def test_lazy_clip_fills_nodata(self):
        """If the source has a numeric nodata, masked pixels must be
        filled with that nodata value (not NaN)."""
        nodata_val = 255
        arr = _make_large_dask_dataarray(
            nx=1200, ny=1200, fill_value=10, nodata=nodata_val
        )
        # Clip to a small diamond – ensures lots of pixels fall outside
        clip_geom = Polygon([(600, 300), (900, 600), (600, 900), (300, 600)])
        result = self.repo._clip_xarr_to_geometry(arr, clip_geom)

        result_np = result.values
        inside_mask = result_np == 10
        nodata_mask = result_np == nodata_val

        # Every pixel should be either the original value or nodata
        assert np.all(
            inside_mask | nodata_mask
        ), "Some pixels are neither the original value nor the nodata fill"
        # Both should be present (diamond doesn't fill the whole bbox)
        assert np.any(inside_mask), "No pixels retained original value"
        assert np.any(nodata_mask), "No pixels were masked to nodata"

    def test_lazy_clip_nan_nodata_leaves_nan(self):
        """When nodata is NaN, masked pixels should remain NaN
        (no fillna with a numeric value)."""
        # Build a float array with NaN nodata directly
        data = da.full((1200, 1200), 3.0, dtype=np.float32, chunks=(400, 400))
        x_coords = np.linspace(0.5, 1199.5, 1200)
        y_coords = np.linspace(1199.5, 0.5, 1200)
        arr = xr.DataArray(data, dims=("y", "x"), coords={"y": y_coords, "x": x_coords})
        arr.rio.write_crs("EPSG:4326", inplace=True)
        arr.rio.write_nodata(np.nan, inplace=True)
        arr.name = "test_band"

        # Use a diamond so corners of the bounding box fall outside
        clip_geom = Polygon([(600, 300), (900, 600), (600, 900), (300, 600)])
        result = self.repo._clip_xarr_to_geometry(arr, clip_geom)

        result_np = result.values
        # Pixels outside the diamond → NaN
        assert np.isnan(result_np).any(), "Expected NaN for masked pixels"
        # Pixels inside the diamond → original value
        assert (result_np[~np.isnan(result_np)] == 3.0).all()

    # --- edge cases -------------------------------------------------------

    def test_lazy_clip_geometry_outside_raster_returns_empty(self):
        """A geometry that doesn't overlap the raster should return
        a zero-size DataArray."""
        arr = _make_large_dask_dataarray(nx=1200, ny=1200)
        clip_geom = box(2000, 2000, 3000, 3000)  # way outside
        result = self.repo._clip_xarr_to_geometry(arr, clip_geom)
        assert result.size == 0

    def test_lazy_clip_returns_dask_backed(self):
        """The result should still be dask-backed (lazy) before .compute()."""
        arr = _make_large_dask_dataarray(nx=1200, ny=1200, fill_value=1)
        clip_geom = box(100, 100, 1100, 1100)
        result = self.repo._clip_xarr_to_geometry(arr, clip_geom)

        assert isinstance(
            result.data, da.Array
        ), "Expected dask array but got an eagerly computed result"

    # --- small-region fallback (rio.clip) ---------------------------------

    def test_small_region_falls_back_to_rio_clip(self):
        """Arrays smaller than 1000 on any axis should use the eager
        rio.clip path and still produce correct results."""
        data = da.full((500, 500), 42, dtype=np.int32, chunks=(250, 250))
        x_coords = np.linspace(0.5, 499.5, 500)
        y_coords = np.linspace(499.5, 0.5, 500)
        arr = xr.DataArray(data, dims=("y", "x"), coords={"y": y_coords, "x": x_coords})
        arr.rio.write_crs("EPSG:4326", inplace=True)
        arr.rio.write_nodata(0, inplace=True)
        arr.name = "small_test"

        clip_geom = box(100, 100, 400, 400)
        result = self.repo._clip_xarr_to_geometry(arr, clip_geom)

        # Should still return something valid
        assert result.sizes["x"] > 0
        assert result.sizes["y"] > 0
