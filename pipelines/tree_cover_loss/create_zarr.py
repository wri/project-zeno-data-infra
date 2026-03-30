from typing import Dict

from pipelines.globals import ANALYTICS_BUCKET, DATA_LAKE_BUCKET
from pipelines.prefect_flows.common_stages import create_zarr_from_tiles

PIPELINE_CHUNK_SIZE = 10_000
OTF_CHUNK_SIZE = 4_000

# Dataset configurations: (tiles geojson path template, zarr output path template)
# The templates use {version} as a placeholder.
DATASETS = {
    "tree_cover_loss": {
        "tiles_template": "s3://{data_lake_bucket}/umd_tree_cover_loss/{version}/raster/epsg-4326/10/40000/year/gdal-geotiff/tiles.geojson",
        "zarr_template": "s3://{analytics_bucket}/zarr/umd-tree-cover-loss/{version}/year.zarr",
    },
    "tree_cover_loss_from_fires": {
        "tiles_template": "s3://{data_lake_bucket}/umd_tree_cover_loss_from_fires/{version}/raster/epsg-4326/10/40000/year/gdal-geotiff/tiles.geojson",
        "zarr_template": "s3://{analytics_bucket}/zarr/umd-tree-cover-loss-from-fires/{version}/year.zarr",
    },
    "drivers": {
        "tiles_template": "s3://{data_lake_bucket}/wri_google_tree_cover_loss_drivers/{version}/raster/epsg-4326/10/40000/category/gdal-geotiff/tiles.geojson",
        "zarr_template": "s3://{analytics_bucket}/zarr/wri-google-tree-cover-loss-drivers/{version}/category.zarr",
    },
}


def create_zarrs(
    tcl_version: str = "v1.12",
    tclf_version: str = "v1.12",
    drivers_version: str = "v1.12",
    overwrite: bool = False,
) -> Dict[str, str]:
    """Create zarr stores for TCL pipeline datasets from tiled GeoTIFFs.

    Each dataset gets two zarr groups in the same store:
      - ``pipeline`` (10,000 px chunks) for batch pipeline processing
      - ``otf``      (4,000 px chunks)  for on-the-fly API queries

    Returns a dict mapping dataset name to its zarr URI.
    """
    versions = {
        "tree_cover_loss": tcl_version,
        "tree_cover_loss_from_fires": tclf_version,
        "drivers": drivers_version,
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
