import pandas as pd
import pytest
from pydantic import ValidationError

from app.analysis.integrated_alerts.query import create_gadm_integrated_alerts_query
from app.domain.analyzers import integrated_alerts_analyzer
from app.domain.analyzers.integrated_alerts_analyzer import IntegratedAlertsAnalyzer
from app.domain.models.analysis import Analysis
from app.models.land_change.integrated_alerts import (
    ANALYTICS_NAME,
    IntegratedAlertsAnalyticsIn,
)


class TestGadmQueryAdm2:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        self.query = create_gadm_integrated_alerts_query(
            ["IDN", "24", "9"], "admin-intdist-alerts"
        )

    def test_select_clause_aliases_to_alert_columns(self):
        expected_clause = (
            "SELECT country, region, subregion, "
            "STRFTIME(intdist_alert_date, '%Y-%m-%d') AS alert_date, "
            "intdist_alert_confidence AS alert_confidence, "
            "SUM(area_ha)::FLOAT AS area_ha"
        )
        assert expected_clause in self.query

    def test_from_clause(self):
        assert "admin-intdist-alerts'" in self.query

    def test_where_clause(self):
        assert "WHERE country = 'IDN' AND region = 24 AND subregion = 9" in self.query

    def test_group_by_clause(self):
        expected_clause = (
            "GROUP BY country, region, subregion, "
            "intdist_alert_date, intdist_alert_confidence"
        )
        assert expected_clause in self.query

    def test_order_by_clause(self):
        expected_clause = (
            "ORDER BY country, region, subregion, "
            "intdist_alert_date, intdist_alert_confidence"
        )
        assert expected_clause in self.query


class TestGadmQueryIso:
    def test_iso_only_groups_by_country(self):
        query = create_gadm_integrated_alerts_query(["IDN"], "admin-intdist-alerts")
        assert "WHERE country = 'IDN'" in query
        assert "GROUP BY country, intdist_alert_date, intdist_alert_confidence" in query


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


def make_analysis(aoi, start_date="2024-01-01", end_date="2024-12-31") -> Analysis:
    metadata = IntegratedAlertsAnalyticsIn(
        aoi=aoi, start_date=start_date, end_date=end_date
    ).model_dump()
    return Analysis(result=None, metadata=metadata, status="pending")


class TestAnalyzerRouting:
    @pytest.mark.asyncio
    async def test_admin_uses_precomputed(self, monkeypatch):
        captured = {}

        async def fake_precomputed(aoi, dask_client, version):
            captured["called"] = "precomputed"
            captured["version"] = version
            return pd.DataFrame({"alert_date": ["2024-06-01"], "area_ha": [1.0]})

        async def fake_otf(*args, **kwargs):
            captured["called"] = "otf"
            return pd.DataFrame()

        monkeypatch.setattr(
            integrated_alerts_analyzer, "get_precomputed_statistics", fake_precomputed
        )
        monkeypatch.setattr(
            integrated_alerts_analyzer, "zonal_statistics_on_aois", fake_otf
        )

        analyzer = IntegratedAlertsAnalyzer(compute_engine=None, input_uris={})
        analysis = make_analysis({"type": "admin", "ids": ["IDN.24.9"]})
        analysis.metadata["_version"] = "v20260101"

        await analyzer.analyze(analysis)

        assert captured["called"] == "precomputed"
        assert captured["version"] == "v20260101"

    @pytest.mark.asyncio
    async def test_non_admin_uses_otf(self, monkeypatch):
        captured = {}

        async def fake_precomputed(*args, **kwargs):
            captured["called"] = "precomputed"
            return pd.DataFrame()

        async def fake_otf(input_uris, aoi, dask_client, version):
            captured["called"] = "otf"
            return pd.DataFrame({"alert_date": ["2024-06-01"], "area_ha": [1.0]})

        monkeypatch.setattr(
            integrated_alerts_analyzer, "get_precomputed_statistics", fake_precomputed
        )
        monkeypatch.setattr(
            integrated_alerts_analyzer, "zonal_statistics_on_aois", fake_otf
        )

        analyzer = IntegratedAlertsAnalyzer(compute_engine=None, input_uris={})
        analysis = make_analysis({"type": "protected_area", "ids": ["555625448"]})

        await analyzer.analyze(analysis)

        assert captured["called"] == "otf"

    @pytest.mark.asyncio
    async def test_filters_by_date_range(self, monkeypatch):
        async def fake_precomputed(aoi, dask_client, version):
            return pd.DataFrame(
                {
                    "alert_date": ["2023-12-31", "2024-06-01", "2025-01-01"],
                    "area_ha": [1.0, 2.0, 3.0],
                }
            )

        monkeypatch.setattr(
            integrated_alerts_analyzer, "get_precomputed_statistics", fake_precomputed
        )

        analyzer = IntegratedAlertsAnalyzer(compute_engine=None, input_uris={})
        analysis = make_analysis(
            {"type": "admin", "ids": ["IDN.24.9"]},
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

        await analyzer.analyze(analysis)

        assert analysis.result["alert_date"] == ["2024-06-01"]
        assert analysis.result["area_ha"] == [2.0]
