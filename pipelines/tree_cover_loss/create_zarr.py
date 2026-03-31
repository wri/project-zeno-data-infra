from typing import Dict

from pipelines.globals import ANALYTICS_BUCKET, DATA_LAKE_BUCKET
from pipelines.prefect_flows.common_stages import create_zarr_from_tiles

PIPELINE_CHUNK_SIZE = 10_000
OTF_CHUNK_SIZE = 4_000

TCL_VERSION = "v1.12"
TCLF_VERSION = "v1.12"
DRIVERS_VERSION = "v1.12"

DATASETS = {
    "tree_cover_loss": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/umd_tree_cover_loss/{TCL_VERSION}/raster/epsg-4326/10/40000/year/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/umd-tree-cover-loss/{TCL_VERSION}/year.zarr",
        "dtype": "uint8",
    },
    "tree_cover_loss_from_fires": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/umd_tree_cover_loss_from_fires/{TCLF_VERSION}/raster/epsg-4326/10/40000/year/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/umd-tree-cover-loss-from-fires/{TCLF_VERSION}/year.zarr",
        "dtype": "uint8",
    },
    "drivers": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/wri_google_tree_cover_loss_drivers/{DRIVERS_VERSION}/raster/epsg-4326/10/40000/category/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/wri-google-tree-cover-loss-drivers/{DRIVERS_VERSION}/category.zarr",
        "dtype": "uint8",
    },
}


def create_zarrs(
    overwrite: bool = False,
) -> Dict[str, str]:
    """Create zarr stores for TCL pipeline datasets from tiled GeoTIFFs.

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
