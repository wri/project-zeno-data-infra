import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio

from app.domain.repositories.alert_points_file import AlertPointsFile

# Shapefile (DBF) field names hold at most 10 characters.
SHAPEFILE_COLUMN_NAMES = {
    "alert_confidence": "confidence",
    "natural_lands_class": "nl_class",
}


class CsvAlertPointsFile(AlertPointsFile):
    """A CSV of alert points, one row per point."""

    extension = "csv"
    content_type = "text/csv"

    def __init__(self, path: Path):
        self.path = path

    def append(self, points: pd.DataFrame) -> None:
        is_first_page = not self.path.exists()
        points.to_csv(self.path, mode="a", header=is_first_page, index=False)

    def close(self) -> None:
        pass


class ShapefileZipAlertPointsFile(AlertPointsFile):
    """A zipped point shapefile of alert points in EPSG:4326, with column names
    shortened to fit the DBF limit."""

    extension = "zip"
    content_type = "application/zip"

    def __init__(self, path: Path):
        self.path = path
        self.shapefile_dir = path.parent / f"{path.stem}-shapefile"
        self.shapefile_dir.mkdir()
        self.shapefile_path = self.shapefile_dir / f"{path.stem}.shp"

    def append(self, points: pd.DataFrame) -> None:
        is_first_page = not self.shapefile_path.exists()
        page = gpd.GeoDataFrame(
            points.rename(columns=SHAPEFILE_COLUMN_NAMES),
            geometry=gpd.points_from_xy(points["lon"], points["lat"]),
            crs="EPSG:4326",
        )
        pyogrio.write_dataframe(
            page,
            self.shapefile_path,
            driver="ESRI Shapefile",
            append=not is_first_page,
        )

    def close(self) -> None:
        with zipfile.ZipFile(self.path, "w", zipfile.ZIP_DEFLATED) as archive:
            for part in sorted(self.shapefile_dir.iterdir()):
                archive.write(part, arcname=part.name)
