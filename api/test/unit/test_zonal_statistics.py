import pytest

from app.domain.analyzers.zonal_statistics.analyzer import ZonalStatisticsAnalyzer
from app.domain.analyzers.zonal_statistics.dask_on_the_fly import (
    DaskOnTheFlyStatistics,
)
from app.domain.analyzers.zonal_statistics.precomputed_admin_source import (
    PrecomputedAdminSource,
)
from app.domain.models.analysis import Analysis


class FakeModel:
    """Stand-in for a pydantic AnalyticsIn with an `aoi`."""

    class _Aoi:
        def __init__(self, type_):
            self.type = type_

    def __init__(self, **metadata):
        self.metadata = metadata
        self.aoi = FakeModel._Aoi(metadata["aoi"]["type"])


class TestZonalStatisticsAnalyzer:
    @pytest.mark.asyncio
    async def test_admin_routes_to_admin_source(self):
        captured = {}

        class FakeAdminSource:
            async def get(self, analytics_in):
                captured["called"] = "admin"
                return {"aoi_id": ["IDN"], "area_ha": [1.0]}

        class FakeOtf:
            async def compute(self, analytics_in):
                captured["called"] = "otf"
                return {}

        analyzer = ZonalStatisticsAnalyzer(
            model=FakeModel,
            admin_source=FakeAdminSource(),
            on_the_fly=FakeOtf(),
            input_uris={"a": "b"},
        )
        analysis = Analysis(
            result=None, metadata={"aoi": {"type": "admin"}}, status="pending"
        )

        await analyzer.analyze(analysis)

        assert captured["called"] == "admin"
        assert analysis.result == {"aoi_id": ["IDN"], "area_ha": [1.0]}

    @pytest.mark.asyncio
    async def test_non_admin_routes_to_on_the_fly(self):
        captured = {}

        class FakeAdminSource:
            async def get(self, analytics_in):
                captured["called"] = "admin"
                return {}

        class FakeOtf:
            async def compute(self, analytics_in):
                captured["called"] = "otf"
                return {"aoi_id": ["x"]}

        analyzer = ZonalStatisticsAnalyzer(
            model=FakeModel,
            admin_source=FakeAdminSource(),
            on_the_fly=FakeOtf(),
            input_uris={"a": "b"},
        )
        analysis = Analysis(
            result=None,
            metadata={"aoi": {"type": "protected_area"}},
            status="pending",
        )

        await analyzer.analyze(analysis)

        assert captured["called"] == "otf"
        assert analysis.result == {"aoi_id": ["x"]}

    def test_thumbprint_uses_input_uris(self):
        analyzer = ZonalStatisticsAnalyzer(
            model=FakeModel, admin_source=None, on_the_fly=None, input_uris={"a": "b"}
        )
        assert analyzer.thumbprint() == analyzer.thumbprint()


class TestPrecomputedAdminSource:
    @pytest.mark.asyncio
    async def test_executes_built_query_and_tags_admin(self):
        captured = {}

        class FakeQueryService:
            async def execute(self, query):
                captured["query"] = query
                return {"aoi_id": ["IDN", "BRA"], "area_ha": [1.0, 2.0]}

        def build_query(analytics_in):
            return f"SELECT for {analytics_in}"

        source = PrecomputedAdminSource(
            query_service=FakeQueryService(), build_query=build_query
        )
        result = await source.get("ANALYTICS_IN")

        assert captured["query"] == "SELECT for ANALYTICS_IN"
        assert result["aoi_type"] == ["admin", "admin"]


class FakeEngine:
    """Records map() calls and resolves the dask-style gather/compute chain."""

    def __init__(self, computed):
        self.computed = computed
        self.map_args = None

    def map(self, fn, *iterables):
        self.map_args = (fn, [list(it) for it in iterables])
        return "futures"

    async def gather(self, value):
        return value

    async def compute(self, value):
        return self.computed


class FakeComputedDf:
    def __init__(self, data):
        self.data = data

    def to_dict(self, orient):
        assert orient == "list"
        return self.data


class TestDaskOnTheFlyStatistics:
    @pytest.mark.asyncio
    async def test_prepares_admin_ids_sorted(self, monkeypatch):
        engine = FakeEngine(FakeComputedDf({"aoi_id": ["BRA", "IDN"]}))

        async def fake_get_geojson(aois):
            return [{"type": "Polygon"}, {"type": "Polygon"}]

        monkeypatch.setattr(
            "app.domain.analyzers.zonal_statistics.dask_on_the_fly.get_geojson",
            fake_get_geojson,
        )
        monkeypatch.setattr(
            "app.domain.analyzers.zonal_statistics.dask_on_the_fly.dd.concat",
            lambda dfs: dfs,
        )

        otf = DaskOnTheFlyStatistics(
            compute_engine=engine,
            input_uris={"u": "v"},
            area_fn=lambda *a, **k: None,
            extract_params=lambda a: {"start_date": "2029-01-01"},
        )

        class AnalyticsIn:
            class Aoi:
                def model_dump(self):
                    return {"type": "protected_area", "ids": ["IDN", "BRA"]}

            aoi = Aoi()

        result = await otf.compute(AnalyticsIn())

        # aoi_list is sorted by id and passed to map alongside geojsons
        _, iterables = engine.map_args
        aoi_list = iterables[0]
        assert [a["id"] for a in aoi_list] == ["BRA", "IDN"]
        assert result == {"aoi_id": ["BRA", "IDN"]}

    @pytest.mark.asyncio
    async def test_prepares_feature_collection_geometries(self, monkeypatch):
        engine = FakeEngine(FakeComputedDf({"aoi_id": ["f1"]}))
        features = [
            {"type": "Feature", "properties": {"id": "f1"}, "geometry": {"g": 1}}
        ]

        async def fake_get_geojson(aois):
            return features

        monkeypatch.setattr(
            "app.domain.analyzers.zonal_statistics.dask_on_the_fly.get_geojson",
            fake_get_geojson,
        )
        monkeypatch.setattr(
            "app.domain.analyzers.zonal_statistics.dask_on_the_fly.dd.concat",
            lambda dfs: dfs,
        )

        otf = DaskOnTheFlyStatistics(
            compute_engine=engine,
            input_uris={"u": "v"},
            area_fn=lambda *a, **k: None,
        )

        class AnalyticsIn:
            class Aoi:
                def model_dump(self):
                    return {
                        "type": "feature_collection",
                        "feature_collection": {"features": features},
                    }

            aoi = Aoi()

        await otf.compute(AnalyticsIn())

        _, iterables = engine.map_args
        aoi_list, geojsons = iterables
        assert aoi_list == features
        assert geojsons == [{"g": 1}]  # geometry extracted from features
