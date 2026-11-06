import pytest
from pydantic import ValidationError

from app.models.land_change.integrated_alerts import (
    ANALYTICS_NAME,
    IntegratedAlertsAnalyticsIn,
)


class TestIntegratedAlertsAnalyticsIn:
    def test_defaults(self):
        analytics_in = IntegratedAlertsAnalyticsIn(
            aoi={"type": "admin", "ids": ["IDN.24.9"]},
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        assert ANALYTICS_NAME == "integrated_alerts"
        assert analytics_in._analytics_name == "integrated_alerts"

    def test_rejects_intersections_field(self):
        with pytest.raises(ValidationError):
            IntegratedAlertsAnalyticsIn(
                aoi={"type": "admin", "ids": ["IDN.24.9"]},
                start_date="2024-01-01",
                end_date="2024-12-31",
                intersections=["driver"],
            )

    def test_rejects_bad_date(self):
        with pytest.raises(ValidationError):
            IntegratedAlertsAnalyticsIn(
                aoi={"type": "admin", "ids": ["IDN.24.9"]},
                start_date="not-a-date",
                end_date="2024-12-31",
            )

    def test_rejects_year_only_date(self):
        # Integrated alerts requires full YYYY-MM-DD dates, not year-only.
        with pytest.raises(ValidationError):
            IntegratedAlertsAnalyticsIn(
                aoi={"type": "admin", "ids": ["IDN.24.9"]},
                start_date="2024",
                end_date="2024-12-31",
            )

    def test_accepts_saved_custom_aois(self):
        analytics_in = IntegratedAlertsAnalyticsIn(
            aoi={"type": "custom_aoi", "ids": ["0b6c1f8e-3d2a-4c55-9a8e-2f4d6b7c9e10"]},
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        assert analytics_in.aoi.type == "custom_aoi"

    def test_land_filter_accepts_natural_lands(self):
        analytics_in = IntegratedAlertsAnalyticsIn(
            aoi={"type": "admin", "ids": ["IDN.24.9"]},
            start_date="2024-01-01",
            end_date="2024-12-31",
            land_filter="natural_lands",
        )
        assert analytics_in.land_filter == "natural_lands"

    def test_land_filter_rejects_unsupported_value(self):
        # Only values that are actually implemented are allowed, so an
        # unsupported filter can't be silently ignored.
        with pytest.raises(ValidationError):
            IntegratedAlertsAnalyticsIn(
                aoi={"type": "admin", "ids": ["IDN.24.9"]},
                start_date="2024-01-01",
                end_date="2024-12-31",
                land_filter="primary_forest",
            )
