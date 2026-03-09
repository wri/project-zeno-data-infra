from typing import List

from pyproj import Geod
from shapely.geometry import shape

_geod = Geod(ellps="WGS84")


def compute_geodesic_area_ha(geometry) -> float:
    """Compute geodesic area of a geometry in hectares.

    Args:
        geometry: A shapely geometry or a GeoJSON-like dict.
    """
    if isinstance(geometry, dict):
        geometry = shape(geometry)
    area, _ = _geod.geometry_area_perimeter(geometry)
    return abs(area) / 10_000  # m² → hectares


def compute_total_feature_collection_area_ha(features: List[dict]) -> float:
    """Compute total geodesic area for a list of GeoJSON feature dicts."""
    return sum(
        compute_geodesic_area_ha(feature.get("geometry", feature))
        for feature in features
    )
