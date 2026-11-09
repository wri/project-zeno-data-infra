import tempfile
import zipfile
from pathlib import Path
from typing import Dict

import geopandas as gpd
import pandas as pd

# Shapefile (DBF) field names hold at most 10 characters.
SHAPEFILE_COLUMN_NAMES = {
    "alert_confidence": "confidence",
    "natural_lands_class": "nl_class",
}


def write_alert_points_shapefile_zip(
    points: Dict[str, list], path: Path, layer: str
) -> None:
    """Write points result columns to a zipped point shapefile in EPSG:4326,
    with columns renamed by SHAPEFILE_COLUMN_NAMES."""
    frame = pd.DataFrame(points)
    layer_frame = gpd.GeoDataFrame(
        frame.rename(columns=SHAPEFILE_COLUMN_NAMES),
        geometry=gpd.points_from_xy(frame["lon"], frame["lat"]),
        crs="EPSG:4326",
    )
    with tempfile.TemporaryDirectory() as shapefile_dir:
        layer_frame.to_file(
            Path(shapefile_dir) / f"{layer}.shp", driver="ESRI Shapefile"
        )
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            for part in sorted(Path(shapefile_dir).iterdir()):
                archive.write(part, arcname=part.name)
