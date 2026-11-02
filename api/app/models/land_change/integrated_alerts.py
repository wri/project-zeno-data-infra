import base64
import binascii
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, List, Literal, Optional, Union

from pydantic import Field, PrivateAttr, field_validator, model_validator

from ..common.analysis import AnalysisStatus, AnalyticsIn
from ..common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
    IndigenousAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
)
from ..common.base import Response, StrictBaseModel

ANALYTICS_NAME = "integrated_alerts"
POINTS_ANALYTICS_NAME = "integrated_alerts_points"
POINTS_DEFAULT_LIMIT = 1000
POINTS_MAX_LIMIT = 10_000
# Raising this also needs one next_cursor per AOI in the result metadata.
POINTS_MAX_AOIS = 1

# Full YYYY-MM-DD dates only; year-only values are not accepted.
DATE_REGEX = r"^\d{4}-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$"

AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigenousAreaOfInterest,
    CustomAreaOfInterest,
]


class IntegratedAlertsAnalyticsIn(AnalyticsIn):
    _analytics_name: str = PrivateAttr(default=ANALYTICS_NAME)
    _version: str = PrivateAttr(default="v20260601")
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    start_date: str = Field(
        ...,
        title="Start Date",
        description="Must be in YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2023-01-01"],
    )
    end_date: str = Field(
        ...,
        title="End Date",
        description="Must be in YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2024-12-31"],
    )
    land_filter: Optional[Literal["natural_lands", "natural_forests"]] = Field(
        default=None,
        title="Land Filter",
        # Keep in sync with LAND_FILTER_CLASSES in app/analysis/common/analysis.py
        description=(
            "Only count alerts on these SBTN natural lands classes: "
            "natural_lands = classes 2-11; "
            "natural_forests = classes 2, 5, 8, 9."
        ),
    )


class IntegratedAlertsAnalytics(StrictBaseModel):
    result: Optional[dict] = {  # column oriented for loading into a dataframe
        "aoi_id": ["BRA.1.12", "BRA.1.12", "BRA.1.12", "BRA.1.12"],
        "aoi_type": ["admin", "admin", "admin", "admin"],
        "alert_date": ["2024-01-01", "2024-01-03", "2024-01-03", "2024-01-03"],
        "alert_confidence": ["low", "low", "high", "highest"],
        "area_ha": [38, 5, 3, 2],
    }
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class IntegratedAlertsAnalyticsResponse(Response):
    data: IntegratedAlertsAnalytics


@dataclass(frozen=True)
class PointsCursor:
    """Where a page of points ended: the alerts version that served it, the last
    point's position (opaque here; it's whatever the points repository uses to
    resume) and the fingerprint of the list it pages through. A position only
    means something for that version and that list, so a cursor pins both."""

    version: str
    position: str
    list_id: str


def encode_cursor(cursor: PointsCursor) -> str:
    payload = json.dumps(
        {"v": cursor.version, "p": cursor.position, "l": cursor.list_id},
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(value: str) -> PointsCursor:
    """The PointsCursor in an opaque cursor string; ValueError if it isn't one."""
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()))
        cursor = PointsCursor(
            version=str(payload["v"]),
            position=str(payload["p"]),
            list_id=str(payload["l"]),
        )
    except (binascii.Error, UnicodeDecodeError, ValueError, KeyError, TypeError):
        raise ValueError("Not a valid points cursor.") from None
    if not cursor.position:
        raise ValueError("Not a valid points cursor.")
    return cursor


class IntegratedAlertsPointsAnalyticsIn(AnalyticsIn):
    _analytics_name: str = PrivateAttr(default=POINTS_ANALYTICS_NAME)
    _version: str = PrivateAttr(default="v20261018")
    aoi: AdminAreaOfInterest = Field(
        ...,
        title="AOI",
        description=(
            "GADM admin area to list alerts in, as for the other analytics "
            f"endpoints. Only GADM areas are supported for now, {POINTS_MAX_AOIS} "
            "per request: one id in `ids`."
        ),
    )
    start_date: Optional[str] = Field(
        default=None,
        title="Start Date",
        description=(
            "Must be in YYYY-MM-DD date format. Only the most recent alerts are "
            "stored (see the result metadata), so earlier dates are left out. "
            "Defaults to the first stored date."
        ),
        pattern=DATE_REGEX,
        examples=["2026-10-01"],
    )
    end_date: Optional[str] = Field(
        default=None,
        title="End Date",
        description=(
            "Must be in YYYY-MM-DD date format. Defaults to the latest alert date."
        ),
        pattern=DATE_REGEX,
        examples=["2026-10-16"],
    )
    alert_confidence: Optional[List[Literal["low", "high", "highest"]]] = Field(
        default=None,
        title="Alert Confidence",
        description="Only list alerts at these confidence levels. Defaults to all.",
        min_length=1,
    )
    land_filter: Optional[Literal["natural_lands", "natural_forests"]] = Field(
        default=None,
        title="Land Filter",
        # Keep in sync with LAND_FILTER_CLASSES in app/analysis/common/analysis.py
        description=(
            "Only list alerts on these SBTN natural lands classes: "
            "natural_lands = classes 2-11; "
            "natural_forests = classes 2, 5, 8, 9."
        ),
    )
    limit: int = Field(
        default=POINTS_DEFAULT_LIMIT,
        ge=1,
        le=POINTS_MAX_LIMIT,
        title="Limit",
        description="Page size: maximum number of alerts listed, newest first.",
    )
    cursor: Optional[str] = Field(
        default=None,
        title="Cursor",
        description=(
            "Omit for the first page. For the next page, pass the `next_cursor` from "
            "the previous page's result metadata, with the same other parameters "
            "(`limit` may change); a cursor from a different list is rejected. "
            "A cursor keeps serving the alerts version its list started on, so pages "
            "stay consistent when a new version is released."
        ),
    )

    @field_validator("cursor", mode="after")
    @classmethod
    def validate_cursor(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            decode_cursor(value)  # raises ValueError for anything malformed
        return value

    @model_validator(mode="after")
    def validate_date_range(self) -> "IntegratedAlertsPointsAnalyticsIn":
        if self.start_date is not None and self.end_date is not None:
            start = datetime.strptime(self.start_date, "%Y-%m-%d").date()
            end = datetime.strptime(self.end_date, "%Y-%m-%d").date()
            if end < start:
                raise ValueError("end_date must be on or after start_date")
        return self

    @model_validator(mode="after")
    def validate_aoi_count(self) -> "IntegratedAlertsPointsAnalyticsIn":
        # One list per request keeps the result bounded by limit, and lets
        # dashboards showing the same area share a cached result.
        if len(self.aoi.ids) > POINTS_MAX_AOIS:
            raise ValueError(
                f"The points endpoint takes {POINTS_MAX_AOIS} AOI per request for "
                "now: send one id."
            )
        return self

    @model_validator(mode="after")
    def validate_cursor_list(self) -> "IntegratedAlertsPointsAnalyticsIn":
        if self.cursor is not None:
            if decode_cursor(self.cursor).list_id != self.list_id():
                raise ValueError(
                    "The cursor is for a different list: send the same aoi, dates, "
                    "alert_confidence and land_filter as the page it came from."
                )
        return self

    def list_id(self) -> str:
        """Fingerprint of the list a request pages through: everything that picks
        its points (aoi, dates, alert_confidence, land_filter), but not the page
        size or position, so pages of one list can use different limits."""

        def iso(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            return datetime.strptime(value, "%Y-%m-%d").date().isoformat()

        payload = {
            "aoi": self.aoi.model_dump(mode="json"),
            "start_date": iso(self.start_date),
            "end_date": iso(self.end_date),
            "alert_confidence": (
                None if self.alert_confidence is None else sorted(self.alert_confidence)
            ),
            "land_filter": self.land_filter,
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode())
        return digest.hexdigest()[:16]


class IntegratedAlertsPoints(StrictBaseModel):
    # column oriented for loading into a dataframe; one row per alert pixel in the
    # request's AOI, with lat/lon at the pixel center
    result: Optional[dict] = {
        "lat": [-3.20155, -3.20145],
        "lon": [-52.20405, -52.20395],
        "alert_date": ["2026-10-16", "2026-10-15"],
        "alert_confidence": ["high", "highest"],
        "natural_lands_class": [2, 2],
    }
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class IntegratedAlertsPointsResponse(Response):
    data: IntegratedAlertsPoints


class IntegratedAlertsLatest(StrictBaseModel):
    alert_date: str = Field(
        ...,
        description="Latest date with any alert, in YYYY-MM-DD format.",
        examples=["2026-10-12"],
    )
    version: str = Field(
        ..., description="Data version the date comes from.", examples=["v20261014"]
    )


class IntegratedAlertsLatestResponse(Response):
    data: IntegratedAlertsLatest
