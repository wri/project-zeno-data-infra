import numpy as np
import pytest
import xarray as xr
from app.analysis.common.analysis import clip_zarr_to_geojson
from rasterio.transform import from_origin
from shapely.geometry import box, mapping


@pytest.fixture
def mock_raster():
    # --- 1) Create 10x10 raster of ones ---
    height = width = 10
    pixel_size = 1.0

    # Let's say the top-left pixel's upper-left corner is at (0, 10) in EPSG:4326
    transform = from_origin(west=0, north=10, xsize=pixel_size, ysize=pixel_size)

    da = xr.DataArray(
        np.ones((height, width), dtype=np.float32),
        dims=("y", "x"),
        coords={"y": np.flip(np.arange(height)), "x": np.arange(width)},
        name="mock_band",
    )

    # --- 2) Attach rio properties ---
    da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
    da = da.rio.write_crs("EPSG:4326")
    da = da.rio.write_transform(transform)
    da = da.rio.write_nodata(0)
    return da


def test_clip_zarr_to_geojson(mock_raster):
    geom = box(5, 5, 10, 10)  #
    geojson_geom = mapping(geom)

    clipped = clip_zarr_to_geojson(mock_raster, geojson_geom)
    assert clipped.rio.bounds() == (5.5, 4.5, 10.5, 10.5)
    assert clipped.sum().data == 20


def test_clip_zarr_to_geojson_out_of_bounds(mock_raster):
    geom = box(15, 15, 20, 20)
    geojson_geom = mapping(geom)

    clipped = clip_zarr_to_geojson(mock_raster, geojson_geom)
    assert clipped.rio.bounds() == (15.5, 14.5, 20.5, 20.5)
    assert clipped.sum().data == 0
