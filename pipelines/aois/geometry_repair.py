import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


def repair_geometries(
    features: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Make every geometry a valid 2D MultiPolygon, keeping only polygon parts.

    Returns the repaired features, without any that could not be repaired or had
    no geometry, and a report with one row per originally invalid feature."""
    features = features.set_geometry(shapely.force_2d(features.geometry.values))
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
    result = result.set_geometry(result.geometry.apply(as_multipolygon))
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


def as_multipolygon(geometry: BaseGeometry) -> MultiPolygon:
    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])
    return geometry
