from test.unit.alert_points import aoi_polygon

import pytest
from pydantic import ValidationError
from shapely.geometry import mapping

from app.models.land_change.integrated_alerts import (
    ANALYTICS_NAME,
    IntegratedAlertsAnalyticsIn,
    IntegratedAlertsPointsAnalyticsIn,
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


class TestIntegratedAlertsPointsAnalyticsIn:
    @pytest.mark.parametrize(
        "aoi",
        [
            {"type": "admin", "ids": ["BRA.1"]},
            {"type": "custom_aoi", "ids": ["0b6c1f8e-3d2a-4c55-9a8e-2f4d6b7c9e10"]},
        ],
    )
    def test_takes_admin_or_saved_custom_aois(self, aoi):
        assert IntegratedAlertsPointsAnalyticsIn(aoi=aoi).aoi.type == aoi["type"]

    @pytest.mark.parametrize(
        "aoi",
        [
            {"type": "protected_area", "ids": ["555758964"]},
            {"type": "key_biodiversity_area", "ids": ["1"]},
            {"type": "indigenous_land", "ids": ["MEX11287"]},
            {
                "type": "feature_collection",
                "feature_collection": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "id": "a",
                            "geometry": mapping(aoi_polygon()),
                        }
                    ],
                },
            },
        ],
    )
    def test_rejects_other_aoi_types(self, aoi):
        with pytest.raises(ValidationError):
            IntegratedAlertsPointsAnalyticsIn(aoi=aoi)
