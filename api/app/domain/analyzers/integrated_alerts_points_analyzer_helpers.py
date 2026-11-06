"""Helpers shared by the integrated alerts points and points export
analyzers."""

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from shapely.geometry.base import BaseGeometry

from app.domain.analyzers.integrated_alerts_analyzer import ALERTS_CONFIDENCE
from app.domain.repositories.alert_points_repository import AlertPointsSource
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.models.land_change.integrated_alerts import IntegratedAlertsPointsFilterIn

CONFIDENCE_CODES = {label: code for code, label in ALERTS_CONFIDENCE.items()}
# Columns of a points result, as served by /points and written to exports
POINTS_RESULT_COLUMNS = [
    "lat",
    "lon",
    "alert_date",
    "alert_confidence",
    "natural_lands_class",
]


async def read_aoi_geometry(
    geometry_repository: AoiGeometryRepository,
    analytics_in: IntegratedAlertsPointsFilterIn,
) -> Tuple[str, BaseGeometry]:
    """Read the id and geometry of the request's one AOI."""
    (aoi_id,) = analytics_in.aoi.ids
    (geometry,), _ = await geometry_repository.load(analytics_in.aoi)
    return aoi_id, geometry


def served_alerts_metadata(
    source: AlertPointsSource, start_date: date, end_date: date
) -> Dict[str, Optional[str]]:
    """Describe the alerts version and the stored and served date ranges, for a
    result's metadata. The served dates are None when nothing overlaps."""
    served = start_date <= end_date
    return {
        "version": source.version,
        "latest_alert_date": source.latest_date.isoformat(),
        "stored_start_date": source.first_date.isoformat(),
        "stored_end_date": source.latest_date.isoformat(),
        "start_date": start_date.isoformat() if served else None,
        "end_date": end_date.isoformat() if served else None,
    }


def format_points(points: pd.DataFrame) -> Dict[str, list]:
    """Convert repository points to result columns (POINTS_RESULT_COLUMNS), with
    ISO dates and confidence labels."""
    # A window holds a few dozen distinct dates, so format those once and index
    # into them rather than formatting every row.
    date_codes, dates = pd.factorize(points["alert_date"])
    iso_dates = np.array([d.isoformat() for d in dates], dtype=object)
    return {
        "lat": points["lat"].tolist(),
        "lon": points["lon"].tolist(),
        "alert_date": iso_dates[date_codes].tolist(),
        "alert_confidence": points["confidence"].map(ALERTS_CONFIDENCE).tolist(),
        "natural_lands_class": points["natural_lands_class"].tolist(),
    }


def confidence_codes(analytics_in) -> List[int]:
    """List the requested confidence codes, all of them when none are given."""
    if analytics_in.alert_confidence is None:
        return list(ALERTS_CONFIDENCE)
    return [CONFIDENCE_CODES[c] for c in analytics_in.alert_confidence]


def _parse_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def served_dates(source: AlertPointsSource, analytics_in) -> Tuple[date, date]:
    """The request's date range clipped to the dates the source stores. start >
    end when they don't overlap."""
    start_date = _parse_date(analytics_in.start_date)
    end_date = _parse_date(analytics_in.end_date)
    return (
        source.first_date if start_date is None else max(start_date, source.first_date),
        source.latest_date if end_date is None else min(end_date, source.latest_date),
    )
