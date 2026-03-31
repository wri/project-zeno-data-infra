from typing import Dict

from pipelines.globals import ANALYTICS_BUCKET, DATA_LAKE_BUCKET
from pipelines.prefect_flows.common_stages import create_zarr_from_tiles

PIPELINE_CHUNK_SIZE = 10_000
OTF_CHUNK_SIZE = 4_000

CARBON_NET_FLUX_VERSION = "v20250430"
CARBON_GROSS_REMOVALS_VERSION = "v20250416"
CARBON_GROSS_EMISSIONS_VERSION = "v20250430"

DATASETS = {
    "carbon_net_flux": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/gfw_forest_carbon_net_flux/{CARBON_NET_FLUX_VERSION}/raster/epsg-4326/10/40000/Mg_CO2e_ha-1/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/gfw-carbon-net-flux/{CARBON_NET_FLUX_VERSION}/Mg_CO2e_ha-1.zarr",
        "dtype": "float64",
    },
    "carbon_gross_removals": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/gfw_forest_carbon_gross_removals/{CARBON_GROSS_REMOVALS_VERSION}/raster/epsg-4326/10/40000/Mg_CO2e_ha-1/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/gfw-carbon-gross-removals/{CARBON_GROSS_REMOVALS_VERSION}/Mg_CO2e_ha-1.zarr",
        "dtype": "float64",
    },
    "carbon_gross_emissions": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/gfw_forest_carbon_gross_emissions/{CARBON_GROSS_EMISSIONS_VERSION}/raster/epsg-4326/10/40000/Mg_CO2e_ha-1/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/gfw-carbon-gross-emissions/{CARBON_GROSS_EMISSIONS_VERSION}/Mg_CO2e_ha-1.zarr",
        "dtype": "float64",
    },
}


def create_zarrs(
    overwrite: bool = False,
) -> Dict[str, str]:
    """Create zarr stores for carbon flux datasets from tiled GeoTIFFs.

    Each dataset gets two zarr groups in the same store:
      - ``pipeline`` (10,000 px chunks) for batch pipeline processing
      - ``otf``      (4,000 px chunks)  for on-the-fly API queries

    Returns a dict mapping dataset name to its zarr URI.
    """
    result_uris: Dict[str, str] = {}

    for name, cfg in DATASETS.items():
        create_zarr_from_tiles(
            cfg["tiles_uri"],
            cfg["zarr_uri"],
            [(PIPELINE_CHUNK_SIZE, "pipeline"), (OTF_CHUNK_SIZE, "otf")],
            overwrite=overwrite,
            dtype=cfg["dtype"],
        )
        result_uris[name] = cfg["zarr_uri"]

    return result_uris
