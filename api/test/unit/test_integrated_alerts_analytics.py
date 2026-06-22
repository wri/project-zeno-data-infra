import pytest
from pydantic import ValidationError

from app.domain.analyzers import integrated_alerts_analyzer
from app.domain.analyzers.integrated_alerts_analyzer import (
    IntegratedAlertsAnalyzer,
    full_date,
)
from app.domain.models.analysis import Analysis
from app.models.land_change.integrated_alerts import (
    ANALYTICS_NAME,
    IntegratedAlertsAnalyticsIn,
)


class FakeQueryService:
    def __init__(self):
        self.query = None

    async def execute(self, query):
        self.query = query
        return {
            "aoi_id": ["IDN.24.9"],
            "alert_date": ["2029-06-01"],
            "alert_confidence": ["high"],
            "area_ha": [1.0],
        }


def make_analysis(aoi, start_date="2029-01-01", end_date="2032-12-31") -> Analysis:
    metadata = IntegratedAlertsAnalyticsIn(
        aoi=aoi, start_date=start_date, end_date=end_date
    ).model_dump()
    return Analysis(result=None, metadata=metadata, status="pending")


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


class TestAnalyzerRouting:
    @pytest.mark.asyncio
    async def test_admin_uses_query_service_with_in_clause_and_normalized_dates(self):
        query_service = FakeQueryService()
        analyzer = IntegratedAlertsAnalyzer(
            duckdb_query_service=query_service, input_uris={}
        )
        analysis = make_analysis(
            {"type": "admin", "ids": ["IDN", "BRA.1"]},
            start_date="2029",
            end_date="2032",
        )

        await analyzer.analyze(analysis)

        # single scan keyed by aoi_id, not a per-id UNION ALL
        assert "UNION ALL" not in query_service.query
        assert "aoi_id IN ('IDN', 'BRA.1')" in query_service.query
        # year-only inputs are normalised to full dates
        assert (
            "intdist_alert_date BETWEEN DATE '2029-01-01' AND DATE '2032-12-31'"
            in query_service.query
        )
        assert analysis.result["aoi_type"] == ["admin"]

    @pytest.mark.asyncio
    async def test_non_admin_routes_to_otf(self, monkeypatch):
        class RoutedToOtf(Exception):
            pass

        class FakeEngine:
            def map(self, *args, **kwargs):
                raise RoutedToOtf()

            async def gather(self, *args, **kwargs):
                raise AssertionError("gather should not be reached")

        async def fake_get_geojson(aois):
            return [{"type": "Polygon", "coordinates": []}]

        monkeypatch.setattr(integrated_alerts_analyzer, "get_geojson", fake_get_geojson)

        analyzer = IntegratedAlertsAnalyzer(
            compute_engine=FakeEngine(), input_uris={"x": "y"}
        )
        analysis = make_analysis({"type": "protected_area", "ids": ["555625448"]})

        with pytest.raises(RoutedToOtf):
            await analyzer.analyze(analysis)
