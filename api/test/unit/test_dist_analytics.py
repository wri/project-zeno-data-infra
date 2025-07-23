import re

import pytest
from app.analysis.common.analysis import (
    get_geojson_from_data_api,
    get_geojson_request_for_data_api,
    get_sql_in_list,
)
from app.analysis.dist_alerts.query import create_gadm_dist_query


class TestGadmQueryAdm2NoIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(["IDN", "24", "9"], "gadm_dist_alerts", [])

    def test_create_gadm_adm2_dist_query_no_intersection_select_clause(self):
        expected_clause = "SELECT country, region, subregion, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_from_clause(self):
        expected_clause = "/tmp/gadm_dist_alerts.parquet'"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_group_by_clause(self):
        expected_clause = (
            "GROUP BY country, region, subregion, alert_date, alert_confidence"
        )
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_order_by_clause(self):
        expected_clause = (
            "ORDER BY country, region, subregion, alert_date, alert_confidence"
        )
        assert expected_clause in self.query


class TestGadmQueryAdm1NoIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(["IDN", "24"], "gadm_dist_alerts", [])

    def test_create_gadm_adm1_dist_query_no_intersection_select_clause(self):
        expected_clause = "SELECT country, region, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_from_clause(self):
        expected_clause = "/tmp/gadm_dist_alerts.parquet'"
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24"
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_group_by_clause(self):
        expected_clause = "GROUP BY country, region, alert_date, alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_order_by_clause(self):
        expected_clause = "ORDER BY country, region, alert_date, alert_confidence"
        assert expected_clause in self.query


class TestGadmQueryIsoNoIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(["IDN"], "gadm_dist_alerts", [])

    def test_create_gadm_iso_dist_query_no_intersection_select_clause(self):
        expected_clause = "SELECT country, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_from_clause(self):
        expected_clause = "/tmp/gadm_dist_alerts.parquet'"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN'"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_group_by_clause(self):
        expected_clause = "GROUP BY country, alert_date, alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_order_by_clause(self):
        expected_clause = "ORDER BY country, alert_date, alert_confidence"
        assert expected_clause in self.query


class TestGadmQueryAdm2NaturalLandsIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(
            ["IDN", "24", "9"], "gadm_dist_alerts_by_natural_lands", ["natural_lands"]
        )

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_select_clause(self):
        expected_clause = "SELECT country, region, subregion, natural_land_class, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_from_clause(self):
        expected_clause = "/tmp/gadm_dist_alerts_by_natural_lands.parquet'"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_group_by_clause(
        self,
    ):
        expected_clause = "GROUP BY country, region, subregion, natural_land_class, alert_date, alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_order_by_clause(
        self,
    ):
        expected_clause = "ORDER BY country, region, subregion, natural_land_class, alert_date, alert_confidence"
        assert expected_clause in self.query


class TestGadmQueryAdm2DriverIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(
            ["IDN", "24", "9"], "gadm_dist_alerts_by_driver", ["driver"]
        )

    def test_create_gadm_adm2_dist_query_drivers_intersection_select_clause(self):
        expected_clause = "SELECT country, region, subregion, ldacs_driver, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_from_clause(self):
        expected_clause = "/tmp/gadm_dist_alerts_by_driver.parquet'"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_group_by_clause(self):
        expected_clause = "GROUP BY country, region, subregion, ldacs_driver, alert_date, alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_order_by_clause(self):
        expected_clause = "ORDER BY country, region, subregion, ldacs_driver, alert_date, alert_confidence"
        assert expected_clause in self.query


def strip_extra_whitespace(string: str) -> str:
    return re.sub(r"\s+", " ", string).strip()


@pytest.mark.asyncio
async def test_get_geojson_from_data_api():
    async def send_request_to_data_api_test(url, params):
        return {
            "data": [
                {
                    "gfw_geojson": '{"type":"MultiPolygon","coordinates":[[[[147.082461217,-37.914061475],[147.079322906,-37.914295051],[147.079382556,-37.913043591],[147.079792868,-37.910992492],[147.07764676,-37.91072299],[147.078204151,-37.907936585],[147.07838676,-37.907876491],[147.082461217,-37.914061475]]]]}'
                }
            ],
            "status": "success",
        }

    aoi = {"type": "protected_area", "ids": ["555625448"]}
    geojson = await get_geojson_from_data_api(
        aoi, send_request=send_request_to_data_api_test
    )
    assert geojson[0]["type"] == "MultiPolygon"


@pytest.mark.asyncio
async def test_get_geojson_from_data_api_failed():
    async def send_request_to_data_api_test_failed(url, params):
        return {"detail": "you suck", "status": "failed"}

    aoi = {"type": "protected_area", "ids": ["555625448"]}
    try:
        await get_geojson_from_data_api(
            aoi, send_request=send_request_to_data_api_test_failed
        )
    except ValueError:
        assert True


def test_get_geojson_request_for_data_api_protected_areas():
    aoi = {"type": "protected_area", "ids": ["555625448"]}
    url, params = get_geojson_request_for_data_api(aoi)
    assert (
        url
        == "https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query"
    )
    assert params == {
        "sql": "select gfw_geojson from data where wdpaid in ('555625448') order by wdpaid"
    }


def test_get_geojson_request_for_data_api_indigenous_lands():
    aoi = {"type": "indigenous_land", "ids": ["8111"]}
    url, params = get_geojson_request_for_data_api(aoi)
    assert (
        url
        == "https://data-api.globalforestwatch.org/dataset/landmark_icls/latest/query"
    )
    assert params == {
        "sql": "select gfw_geojson from data where objectid in ('8111') order by objectid"
    }


def test_get_geojson_request_for_data_api_kba():
    aoi = {"type": "key_biodiversity_area", "ids": ["1241"]}
    url, params = get_geojson_request_for_data_api(aoi)
    assert (
        url
        == "https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query"
    )
    assert params == {
        "sql": "select gfw_geojson from data where sitrecid in ('1241') order by sitrecid"
    }


def test_get_geojson_request_for_data_api_notreal():
    aoi = {"type": "notreal", "ids": ["1241"]}

    # Verify ValueError is raised
    with pytest.raises(ValueError) as exc_info:
        get_geojson_request_for_data_api(aoi)

    # Optional: Verify exception message
    assert "Unable to retrieve AOI type notreal from Data API." in str(exc_info.value)


def test_sql_in_list():
    assert get_sql_in_list(["1", "2", "3"]) == "('1', '2', '3')"
