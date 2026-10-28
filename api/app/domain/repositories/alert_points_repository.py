from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import pandas as pd
from shapely.geometry.base import BaseGeometry

# Columns of AlertPointsPage.points: pixel-center latitude and longitude,
# alert_date (datetime.date), confidence code (2=low, 3=high, 4=highest) and SBTN
# natural lands class.
POINT_COLUMNS = ["lat", "lon", "alert_date", "confidence", "natural_lands_class"]


@dataclass(frozen=True)
class AlertPointsSource:
    """The alerts a repository serves: one alerts version, with every alert from
    first_date to latest_date stored."""

    version: str
    first_date: date
    latest_date: date


@dataclass(frozen=True)
class AlertPointsQuery:
    """Alert points inside geometry, dated start_date to end_date (both stored),
    at the given confidence codes and, unless natural_lands_classes is None, on
    those classes. after is the last_position of the previous page."""

    geometry: BaseGeometry
    start_date: date
    end_date: date
    confidences: List[int]
    natural_lands_classes: Optional[List[int]]
    limit: int
    after: Optional[str] = None


@dataclass(frozen=True)
class AlertPointsPage:
    """Up to limit points (POINT_COLUMNS), newest date first in an order that is
    the same for every request; whether more points matched; and the position of
    the last point, opaque to callers, to pass as after for the next page."""

    points: pd.DataFrame
    more: bool
    last_position: Optional[str]


class AlertPointsRepository(ABC):
    @abstractmethod
    async def source(self) -> AlertPointsSource:
        pass

    @abstractmethod
    async def read_most_recent_points(self, query: AlertPointsQuery) -> AlertPointsPage:
        """Raises ValueError if query.after isn't a position from this
        repository."""
        pass
