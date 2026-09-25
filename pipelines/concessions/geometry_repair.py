import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


def repair_geometries(
    features: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Make invalid geometries valid, keeping only their polygon parts.

    Returns the repaired features, without any that could not be repaired, and
    a report with one row per originally invalid feature."""
    invalid = features[~features.is_valid]
    repaired_geometries = invalid.geometry.make_valid().apply(polygonal_part)
    repaired = repaired_geometries.is_valid & ~repaired_geometries.is_empty

    report = pd.DataFrame(
        {
            "aoi_id": invalid.aoi_id,
            "invalid_reason": shapely.is_valid_reason(invalid.geometry),
            "repaired": repaired,
        }
    ).reset_index(drop=True)

    result = features.copy()
    result.loc[invalid.index, "geometry"] = repaired_geometries
    result = result.drop(index=invalid.index[~repaired])
    return result, report


def polygonal_part(geometry: BaseGeometry) -> BaseGeometry:
    if isinstance(geometry, (Polygon, MultiPolygon)):
        return geometry
    polygons = [
        part
        for part in getattr(geometry, "geoms", [])
        if isinstance(part, (Polygon, MultiPolygon))
    ]
    return shapely.union_all(polygons) if polygons else Polygon()
