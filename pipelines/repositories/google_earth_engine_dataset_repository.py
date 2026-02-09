import ee
import xarray as xr
from shapely.geometry import mapping


class GoogleEarthEngineDatasetRepository:
    # lazy load ee assets since ee may not be initiazed yet
    EE_ASSETS = {
        "loss": lambda: ee.Image("UMD/hansen/global_forest_change_2024_v1_12"),
        "tcl_drivers": lambda: ee.Image(
            "projects/landandcarbon/assets/wri_gdm_drivers_forest_loss_1km/v1_2_2001_2024"
        ),
        "area": lambda: ee.Image.pixelArea(),
    }

    def __init__(self, default_scale=0.00025, default_projection="EPSG:4326"):
        self.default_scale = default_scale
        self.default_projection = default_projection

    def load(self, dataset, geometry, like=None):
        ee.Initialize()

        # must wrap geometry in ee Geometry object to use
        ee_geom = ee.Geometry(mapping(geometry))

        # read dataset clipped to geometry, remove time dimension added by default from EE
        ds = xr.open_dataset(
            self.EE_ASSETS[dataset]().clip(ee_geom),
            engine="ee",
            geometry=ee_geom,
            crs=self.default_projection,
            scale=self.default_scale,
        ).squeeze("time")

        if like is not None:
            return ds.reindex_like(like, method="nearest")

        return ds
