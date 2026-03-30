from typing import Dict

from pipelines.globals import ANALYTICS_BUCKET, DATA_LAKE_BUCKET
from pipelines.prefect_flows.common_stages import create_zarr_from_tiles

PIPELINE_CHUNK_SIZE = 10_000
OTF_CHUNK_SIZE = 4_000

DATASETS = {
    "carbon_net_flux": {
        "tiles_template": "s3://{data_lake_bucket}/gfw_forest_carbon_net_flux/{version}/raster/epsg-4326/10/40000/Mg_CO2e_ha-1/gdal-geotiff/tiles.geojson",
        "zarr_template": "s3://{analytics_bucket}/zarr/gfw-carbon-net-flux/{version}/Mg_CO2e_ha-1.zarr",
    },
    "carbon_gross_removals": {
        "tiles_template": "s3://{data_lake_bucket}/gfw_forest_carbon_gross_removals/{version}/raster/epsg-4326/10/40000/Mg_CO2e_ha-1/gdal-geotiff/tiles.geojson",
        "zarr_template": "s3://{analytics_bucket}/zarr/gfw-carbon-gross-removals/{version}/Mg_CO2e_ha-1.zarr",
    },
    "carbon_gross_emissions": {
        "tiles_template": "s3://{data_lake_bucket}/gfw_forest_carbon_gross_emissions/{version}/raster/epsg-4326/10/40000/Mg_CO2e_ha-1/gdal-geotiff/tiles.geojson",
        "zarr_template": "s3://{analytics_bucket}/zarr/gfw-carbon-gross-emissions/{version}/Mg_CO2e_ha-1.zarr",
    },
}


def create_zarrs(
    carbon_net_flux_version: str = "v20250430",
    carbon_gross_removals_version: str = "v20250416",
    carbon_gross_emissions_version: str = "v20250430",
    overwrite: bool = False,
) -> Dict[str, str]:
    """Create zarr stores for carbon flux datasets from tiled GeoTIFFs.

    Each dataset gets two zarr groups in the same store:
      - ``pipeline`` (10,000 px chunks) for batch pipeline processing
      - ``otf``      (4,000 px chunks)  for on-the-fly API queries

    Returns a dict mapping dataset name to its zarr URI.
    """
    versions = {
        "carbon_net_flux": carbon_net_flux_version,
        "carbon_gross_removals": carbon_gross_removals_version,
        "carbon_gross_emissions": carbon_gross_emissions_version,
    }

    result_uris: Dict[str, str] = {}

    for name, cfg in DATASETS.items():
        version = versions[name]
        tiles_uri = cfg["tiles_template"].format(
            data_lake_bucket=DATA_LAKE_BUCKET, version=version
        )
        zarr_uri = cfg["zarr_template"].format(
            analytics_bucket=ANALYTICS_BUCKET, version=version
        )

        create_zarr_from_tiles(
            tiles_uri,
            zarr_uri,
            PIPELINE_CHUNK_SIZE,
            group="pipeline",
            overwrite=overwrite,
        )
        create_zarr_from_tiles(
            tiles_uri, zarr_uri, OTF_CHUNK_SIZE, group="otf", overwrite=overwrite
        )

        result_uris[name] = zarr_uri

    return result_uris
