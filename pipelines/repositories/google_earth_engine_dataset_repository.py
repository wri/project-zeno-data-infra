import base64
import json
import os

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
        # initialize lazily if not already initialized
        self.initialize()

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

    def initialize(self):
        if not ee.data.is_initialized():
            gee_service_account_b64 = os.getenv("GEE_SERVICE_ACCOUNT_JSON")
            if gee_service_account_b64:
                gee_service_account = json.loads(
                    base64.b64decode(gee_service_account_b64).decode("utf-8")
                )
                creds = ee.ServiceAccountCredentials(
                    gee_service_account["client_email"],
                    key_data=json.dumps(gee_service_account),
                )
                ee.Initialize(
                    creds, opt_url="https://earthengine-highvolume.googleapis.com"
                )
            else:
                raise RuntimeError("No valid EE credentials found")
