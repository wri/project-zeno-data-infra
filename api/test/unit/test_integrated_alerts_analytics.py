import pytest
from pydantic import ValidationError

from app.domain.analyzers.integrated_alerts_analyzer import (
    build_admin_query,
    full_date,
    gadm_subquery,
)
from app.models.land_change.integrated_alerts import (
    ANALYTICS_NAME,
    IntegratedAlertsAnalyticsIn,
)


class TestGadmSubquery:
    def test_adm2_filters_and_groups(self):
        query = gadm_subquery("IDN.24.9", "2029-01-01", "2032-12-31")
        assert "SELECT 'IDN.24.9' AS aoi_id" in query
        assert "country = 'IDN' AND region = 24 AND subregion = 9" in query
        assert (
            "intdist_alert_date BETWEEN DATE '2029-01-01' AND DATE '2032-12-31'"
            in query
        )
        assert "GROUP BY intdist_alert_date, intdist_alert_confidence" in query
        assert "STRFTIME(intdist_alert_date, '%Y-%m-%d') AS alert_date" in query
        assert "intdist_alert_confidence AS alert_confidence" in query
        assert "SUM(area_ha)::FLOAT AS area_ha" in query

    def test_iso_only_filters_by_country(self):
        query = gadm_subquery("IDN", "2029-01-01", "2032-12-31")
        assert "WHERE country = 'IDN' AND intdist_alert_date" in query
        assert "region" not in query
        assert "subregion" not in query


class TestBuildAdminQuery:
    def test_unions_per_id_with_normalized_dates_and_ordering(self):
        analytics_in = IntegratedAlertsAnalyticsIn(
            aoi={"type": "admin", "ids": ["IDN", "BRA.1"]},
            start_date="2029",
            end_date="2032",
        )

        query = build_admin_query(analytics_in)

        assert "UNION ALL" in query
        assert "SELECT 'IDN' AS aoi_id" in query
        assert "SELECT 'BRA.1' AS aoi_id" in query
        # year-only inputs are normalised to full dates
        assert "DATE '2029-01-01' AND DATE '2032-12-31'" in query
        assert query.rstrip().endswith("ORDER BY aoi_id, alert_date, alert_confidence")


class TestFullDate:
    def test_year_only_start_and_end(self):
        assert full_date("2029") == "2029-01-01"
        assert full_date("2029", end=True) == "2029-12-31"

    def test_full_date_passthrough(self):
        assert full_date("2029-03-16") == "2029-03-16"
        assert full_date("2029-03-16", end=True) == "2029-03-16"


class TestIntegratedAlertsModel:
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
