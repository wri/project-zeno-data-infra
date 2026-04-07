from functools import partial
from typing import Any, Dict

import dask.dataframe as dd
import newrelic.agent as nr_agent
import numpy as np
from flox.xarray import xarray_reduce

from app.analysis.common.analysis import get_geojson, read_zarr_clipped_to_geojson
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.land_change.natural_lands import NaturalLandsAnalyticsIn

NATURAL_LANDS_CLASSES = {
    2: "Natural forests",
    3: "Natural short vegetation",
    4: "Natural water",
    5: "Mangroves",
    6: "Bare",
    7: "Snow",
    8: "Wetland natural forests",
    9: "Natural peat forests",
    10: "Wetland natural short vegetation",
    11: "Natural peat short vegetation",
    12: "Cropland",
    13: "Built-up",
    14: "Non-natural tree cover",
    15: "Non-natural short vegetation",
    16: "Non-natural water",
    17: "Wetland non-natural tree cover",
    18: "Non-natural peat tree cover",
    19: "Wetland non-natural short vegetation",
    20: "Non-natural peat short vegetation",
    21: "Non-natural bare",
}

INPUT_URIS = {
    Environment.production: {
        "natural_lands_zarr_uri": (
            "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr"
        ),
        "pixel_area_zarr_uri": "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr/",
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/admin-natural-lands.parquet",
    }
}


class NaturalLandsAnalyzer(Analyzer):
    """Get the natural lands areas by class for the input AOIs"""

    def __init__(
        self,
        compute_engine=None,
        input_uris: Dict[str, str] | None = None,
    ):
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.input_uris = input_uris

    @nr_agent.function_trace(name="NaturalLandsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        natural_lands_analytics_in = NaturalLandsAnalyticsIn(**analysis.metadata)
        if analysis.metadata.get("_input_uris") is not None:
            natural_lands_analytics_in._input_uris = analysis.metadata["_input_uris"]
        if natural_lands_analytics_in.aoi.type == "admin":
            gadm_ids = natural_lands_analytics_in.aoi.ids
            results = await self.analyze_admin_areas(gadm_ids)
        else:
            aois = natural_lands_analytics_in.aoi.model_dump()
            geojsons = await get_geojson(aois)
            if aois["type"] != "feature_collection":
                aoi_list = sorted(
                    [{"type": aois["type"], "id": id} for id in aois["ids"]],
                    key=lambda aoi: aoi["id"],
                )
            else:
                aoi_list = aois["feature_collection"]["features"]
                geojsons = [geojson["geometry"] for geojson in geojsons]

            analysis_partial = partial(self.analyze_area, self.input_uris)
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
            results = combined_results_df.to_dict(orient="list")

        analysis.result = results

    async def analyze_admin_areas(self, gadm_ids) -> Dict[str, Any]:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        id_str = (", ").join([f"'{aoi_id}'" for aoi_id in gadm_ids])
        query = f"select natural_lands_class, area_ha, aoi_id from data_source where aoi_id in ({id_str})"
        query_service = DuckDbPrecalcQueryService(self.input_uris["admin_results_uri"])
        df = await query_service.execute(query)
        df["aoi_type"] = ["admin"] * len(df["aoi_id"])

        return df

    @staticmethod
    def analyze_area(input_uris: Dict[str, str], aoi, geojson) -> dd.DataFrame:
        natural_lands_obj_name = input_uris["natural_lands_zarr_uri"]
        pixel_area_obj_name = input_uris["pixel_area_zarr_uri"]

        natural_lands = read_zarr_clipped_to_geojson(
            natural_lands_obj_name, geojson
        ).band_data
        natural_lands.name = "natural_lands_class"

        pixel_area = read_zarr_clipped_to_geojson(pixel_area_obj_name, geojson)

        groupby_layers = [natural_lands]
        expected_groups = [np.arange(1, 22)]

        counts = xarray_reduce(
            pixel_area,
            *tuple(groupby_layers),
            func="sum",
            expected_groups=tuple(expected_groups),
        )

        df = (
            counts.to_dask_dataframe()
            .drop("band", axis=1)
            .drop("spatial_ref", axis=1)
            .rename(columns={"band_data": "area_ha"})
        )

        df["aoi_type"] = aoi["type"].lower()
        df["aoi_id"] = aoi["id"] if "id" in aoi else aoi["properties"]["id"]

        df["natural_lands_class"] = df.natural_lands_class.apply(
            lambda x: NATURAL_LANDS_CLASSES.get(x, "Unclassified")
        )

        df = df[df.area_ha > 0]
        return df
