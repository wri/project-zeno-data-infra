from unittest.mock import patch

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from pipelines.globals import land_ghg_inventory_agriculture_zarr_uri
from pipelines.land_ghg_inventory import create_agriculture_zarr as mod
from pipelines.land_ghg_inventory.agriculture_stages import AGRICULTURE_SOURCE_VARS


@pytest.fixture
def reference_veg_dataset():
    """4x4 vegetation grid, 2 years -- only used for its geobox."""
    ref = xr.DataArray(
        da.from_array(np.zeros((2, 4, 4), dtype="float32"), chunks=(1, 4, 4)),
        dims=["year", "y", "x"],
        coords={
            "year": [0, 1],
            "y": [1.0, 0.5, 0.0, -0.5],
            "x": [0.0, 0.5, 1.0, 1.5],
        },
    )
    return xr.Dataset({mod.REFERENCE_GRID_VAR: ref})


def _fake_cropland_cog(value):
    """A coarser source raster (absolute kg total per pixel), matching the
    reference grid 2:1 per axis so each source pixel has exactly 4 children.
    Pixel centers are offset (0.75/-0.25, 0.25/1.25) rather than aligned with
    the reference grid's origin, so its bounds (-0.5..1.5) fully cover the
    reference grid's bounds (-0.25..1.75, -0.75..1.25) -- ``_resample_total_uniformly``
    assumes full coverage, as the real global-to-global grids have."""
    arr = xr.DataArray(
        da.from_array(np.full((1, 2, 2), value, dtype="float32"), chunks=(1, 2, 2)),
        dims=["band", "y", "x"],
        coords={"band": [1], "y": [0.75, -0.25], "x": [0.25, 1.25]},
    )
    arr.rio.write_crs("EPSG:4326", inplace=True)
    return arr


def _fake_livestock_cog(value):
    """A coarser source raster (kg/ha rate), band dim included like a real GeoTIFF read."""
    arr = xr.DataArray(
        da.from_array(np.full((1, 2, 2), value, dtype="float32"), chunks=(1, 2, 2)),
        dims=["band", "y", "x"],
        coords={"band": [1], "y": [1.0, 0.0], "x": [0.0, 1.0]},
    )
    arr.rio.write_crs("EPSG:4326", inplace=True)
    return arr


@pytest.fixture
def pixel_area_layer():
    """4x4 pixel-area layer (hectares), matching the reference grid 1:1 --
    the shape ``common.align_to`` returns after loading + snapping."""
    return xr.DataArray(
        da.from_array(np.full((4, 4), 5.0, dtype="float64"), chunks=(4, 4)),
        dims=["y", "x"],
        coords={
            "y": [1.0, 0.5, 0.0, -0.5],
            "x": [0.0, 0.5, 1.0, 1.5],
        },
    )


def test_create_agriculture_zarr_writes_expected_shape(
    reference_veg_dataset, pixel_area_layer
):
    captured = {}

    def fake_to_zarr(self, uri, group=None, mode=None):
        captured["ds"] = self
        captured["uri"] = uri
        captured["group"] = group
        captured["mode"] = mode

    with (
        patch.object(mod, "s3_uri_exists", return_value=False),
        patch.object(mod.xr, "open_zarr", return_value=reference_veg_dataset),
        patch.object(
            mod.rio,
            "open_rasterio",
            side_effect=[_fake_cropland_cog(400.0), _fake_livestock_cog(2_000.0)],
        ),
        patch.object(mod, "align_to", return_value=pixel_area_layer) as mock_align_to,
        patch.object(xr.Dataset, "to_zarr", fake_to_zarr),
    ):
        result_uri = mod.create_agriculture_zarr(overwrite=False)

    assert result_uri == land_ghg_inventory_agriculture_zarr_uri
    assert captured["uri"] == land_ghg_inventory_agriculture_zarr_uri
    assert captured["group"] == "pipeline"
    assert captured["mode"] == "w"

    # pixel area is only needed for livestock now (cropland uses the
    # mass-conserving uniform-split path instead of an area multiply).
    mock_align_to.assert_called_once()

    ds = captured["ds"].compute()
    # matches what agriculture_stages.load_agriculture expects: cropland +
    # livestock variables, dims (y, x) with no leftover band dim.
    assert set(ds.data_vars) == set(AGRICULTURE_SOURCE_VARS.values())
    assert ds.sizes.keys() == {"y", "x"}
    assert "band" not in ds.dims

    # resampled onto the reference (vegetation) grid, not the source grid
    assert list(ds.y.values) == [1.0, 0.5, 0.0, -0.5]
    assert list(ds.x.values) == [0.0, 0.5, 1.0, 1.5]

    # cropland: absolute per-pixel total (400 kg), divided by the (here,
    # exact: 1.0/0.5 = 2 per axis) child count per source pixel -- 4 children
    # each get 400/4=100 kg, then kg -> Mg: 0.1. Each 2x2 block of the 4x4
    # destination grid corresponds to one of the 4 (identical-valued) source
    # pixels, so the grand total is 4 source pixels x 0.4 Mg.
    cropland = ds[AGRICULTURE_SOURCE_VARS["cropland"]].values
    assert cropland[0, 0] == pytest.approx(400.0 / 4 / mod.KG_PER_MG)
    assert np.nansum(cropland) == pytest.approx(4 * 400.0 / mod.KG_PER_MG)

    # livestock: kg/ha -> ha-multiplied -> Mg conversion applied:
    # 2_000 kg/ha * 5 ha / 1000 = 10
    livestock = ds[AGRICULTURE_SOURCE_VARS["livestock"]].values
    assert livestock[0, 0] == pytest.approx(10.0)


def test_create_agriculture_zarr_skips_when_present_and_not_overwrite():
    with (
        patch.object(mod, "s3_uri_exists", return_value=True) as mock_exists,
        patch.object(mod, "_reference_geobox") as mock_geobox,
    ):
        result_uri = mod.create_agriculture_zarr(overwrite=False)

    assert result_uri == land_ghg_inventory_agriculture_zarr_uri
    mock_exists.assert_called_once_with(
        f"{land_ghg_inventory_agriculture_zarr_uri}/pipeline/zarr.json"
    )
    mock_geobox.assert_not_called()


def test_create_agriculture_zarr_overwrite_skips_exists_check(
    reference_veg_dataset, pixel_area_layer
):
    with (
        patch.object(mod, "s3_uri_exists") as mock_exists,
        patch.object(mod.xr, "open_zarr", return_value=reference_veg_dataset),
        patch.object(
            mod.rio,
            "open_rasterio",
            side_effect=[_fake_cropland_cog(100.0), _fake_livestock_cog(500.0)],
        ),
        patch.object(mod, "align_to", return_value=pixel_area_layer),
        patch.object(xr.Dataset, "to_zarr"),
    ):
        mod.create_agriculture_zarr(overwrite=True)

    mock_exists.assert_not_called()


def test_resample_total_uniformly_conserves_mass_per_source_pixel(
    reference_veg_dataset,
):
    """Each source pixel keeps a distinct value, so that summing the
    reprojected output over just the children of one source pixel (not the
    whole grid) is a meaningful check -- verifies conservation isn't masked
    by every source pixel happening to carry the same value. This fixture's
    resolution ratio (1.0 source / 0.5 destination = 2 per axis, 4 children
    per source pixel) is an exact integer, so conservation holds exactly per
    source pixel here (the real cropland grids' ratio isn't a whole number,
    so real per-pixel conservation is only approximate -- see the module
    docstring)."""
    distinct_cog = xr.DataArray(
        da.from_array(
            np.array([[100.0, 200.0], [300.0, 400.0]], dtype="float32").reshape(
                1, 2, 2
            ),
            chunks=(1, 2, 2),
        ),
        dims=["band", "y", "x"],
        coords={"band": [1], "y": [0.75, -0.25], "x": [0.25, 1.25]},
    )
    distinct_cog.rio.write_crs("EPSG:4326", inplace=True)

    with patch.object(mod.xr, "open_zarr", return_value=reference_veg_dataset):
        geobox = mod._reference_geobox()

    with patch.object(mod.rio, "open_rasterio", return_value=distinct_cog):
        result = mod._resample_total_uniformly("fake_uri", geobox).compute()

    # source pixel (0,0)=100 (covers y=[1.25,0.25], x=[-0.25,0.75]) ->
    # destination (0,0),(0,1),(1,0),(1,1) (4 children)
    assert np.nansum(result.values[0:2, 0:2]) == pytest.approx(100.0)
    # source pixel (0,1)=200 -> destination (0,2),(0,3),(1,2),(1,3)
    assert np.nansum(result.values[0:2, 2:4]) == pytest.approx(200.0)
    # source pixel (1,0)=300 -> destination (2,0),(2,1),(3,0),(3,1)
    assert np.nansum(result.values[2:4, 0:2]) == pytest.approx(300.0)
    # source pixel (1,1)=400 -> destination (2,2),(2,3),(3,2),(3,3)
    assert np.nansum(result.values[2:4, 2:4]) == pytest.approx(400.0)
    assert np.nansum(result.values) == pytest.approx(100.0 + 200.0 + 300.0 + 400.0)
