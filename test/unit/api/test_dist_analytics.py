import re

import pytest

from api.app.analysis import get_geojson_from_data_api, get_geojson_request_for_data_api
from api.app.query import create_gadm_dist_query


def test_create_gadm_adm2_dist_query_no_intersection_select_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], [])
    expected_clause = "SELECT country, region, subregion, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_no_intersection_from_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], [])
    expected_clause = "FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts.parquet'"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_no_intersection_where_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], [])
    expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_no_intersection_group_by_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], [])
    expected_clause = (
        "GROUP BY country, region, subregion, alert_date, alert_confidence"
    )
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_no_intersection_order_by_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], [])
    expected_clause = (
        "ORDER BY country, region, subregion, alert_date, alert_confidence"
    )
    assert expected_clause in query


def test_create_gadm_adm1_dist_query_no_intersection_select_clause():
    query = create_gadm_dist_query(["IDN", "24"], [])
    expected_clause = "SELECT country, region, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
    assert expected_clause in query


def test_create_gadm_adm1_dist_query_no_intersection_from_clause():
    query = create_gadm_dist_query(["IDN", "24"], [])
    expected_clause = "FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts.parquet'"
    assert expected_clause in query


def test_create_gadm_adm1_dist_query_no_intersection_where_clause():
    query = create_gadm_dist_query(["IDN", "24"], [])
    expected_clause = "WHERE country = 'IDN' AND region = 24"
    assert expected_clause in query


def test_create_gadm_adm1_dist_query_no_intersection_group_by_clause():
    query = create_gadm_dist_query(["IDN", "24"], [])
    expected_clause = "GROUP BY country, region, alert_date, alert_confidence"
    assert expected_clause in query


def test_create_gadm_adm1_dist_query_no_intersection_order_by_clause():
    query = create_gadm_dist_query(["IDN", "24"], [])
    expected_clause = "ORDER BY country, region, alert_date, alert_confidence"
    assert expected_clause in query


def test_create_gadm_iso_dist_query_no_intersection_select_clause():
    query = create_gadm_dist_query(["IDN"], [])
    expected_clause = "SELECT country, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
    assert expected_clause in query


def test_create_gadm_iso_dist_query_no_intersection_from_clause():
    query = create_gadm_dist_query(["IDN"], [])
    expected_clause = "FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts.parquet'"
    assert expected_clause in query


def test_create_gadm_iso_dist_query_no_intersection_where_clause():
    query = create_gadm_dist_query(["IDN"], [])
    expected_clause = "WHERE country = 'IDN'"
    assert expected_clause in query


def test_create_gadm_iso_dist_query_no_intersection_group_by_clause():
    query = create_gadm_dist_query(["IDN"], [])
    expected_clause = "GROUP BY country, alert_date, alert_confidence"
    assert expected_clause in query


def test_create_gadm_iso_dist_query_no_intersection_order_by_clause():
    query = create_gadm_dist_query(["IDN"], [])
    expected_clause = "ORDER BY country, alert_date, alert_confidence"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_natural_lands_intersection_select_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["natural_lands"])
    expected_clause = "SELECT country, region, subregion, natural_land_class, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_natural_lands_intersection_from_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["natural_lands"])
    expected_clause = "FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts_by_natural_lands.parquet'"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_natural_lands_intersection_where_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["natural_lands"])
    expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_natural_lands_intersection_group_by_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["natural_lands"])
    expected_clause = "GROUP BY country, region, subregion, natural_land_class, alert_date, alert_confidence"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_natural_lands_intersection_order_by_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["natural_lands"])
    expected_clause = "ORDER BY country, region, subregion, natural_land_class, alert_date, alert_confidence"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_drivers_intersection_select_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["driver"])
    expected_clause = "SELECT country, region, subregion, ldacs_driver, STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_drivers_intersection_from_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["driver"])
    expected_clause = "FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts_by_driver.parquet'"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_drivers_intersection_where_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["driver"])
    expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_drivers_intersection_group_by_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["driver"])
    expected_clause = "GROUP BY country, region, subregion, ldacs_driver, alert_date, alert_confidence"
    assert expected_clause in query


def test_create_gadm_adm2_dist_query_drivers_intersection_order_by_clause():
    query = create_gadm_dist_query(["IDN", "24", "9"], ["driver"])
    expected_clause = "ORDER BY country, region, subregion, ldacs_driver, alert_date, alert_confidence"
    assert expected_clause in query


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

    aoi = {"type": "protected_area", "id": "555625448"}
    geojson = await get_geojson_from_data_api(
        aoi, send_request=send_request_to_data_api_test
    )
    assert geojson["type"] == "MultiPolygon"


@pytest.mark.asyncio
async def test_get_geojson_from_data_api_failed():
    async def send_request_to_data_api_test_failed(url, params):
        return {"detail": "you suck", "status": "failed"}

    aoi = {"type": "protected_area", "id": "555625448"}
    try:
        await get_geojson_from_data_api(
            aoi, send_request=send_request_to_data_api_test_failed
        )
    except ValueError:
        assert True


def test_get_geojson_request_for_data_api_protected_areas():
    aoi = {"type": "protected_area", "id": "555625448"}
    url, params = get_geojson_request_for_data_api(aoi)
    assert (
        url
        == "https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query"
    )
    assert params == {"sql": "select gfw_geojson from data where wdpaid = 555625448"}


def test_get_geojson_request_for_data_api_indigenous_lands():
    aoi = {"type": "indigenous_land", "id": "8111"}
    url, params = get_geojson_request_for_data_api(aoi)
    assert (
        url
        == "https://data-api.globalforestwatch.org/dataset/landmark_icls/latest/query"
    )
    assert params == {"sql": "select gfw_geojson from data where objectid = 8111"}


def test_get_geojson_request_for_data_api_kba():
    aoi = {"type": "key_biodiversity_area", "id": "1241"}
    url, params = get_geojson_request_for_data_api(aoi)
    assert (
        url
        == "https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query"
    )
    assert params == {"sql": "select gfw_geojson from data where sitrecid = 1241"}


def test_get_geojson_request_for_data_api_notreal():
    aoi = {"type": "notreal", "id": "1241"}

    # Verify ValueError is raised
    with pytest.raises(ValueError) as exc_info:
        get_geojson_request_for_data_api(aoi)

    # Optional: Verify exception message
    assert "Unable to retrieve AOI type notreal from Data API." in str(exc_info.value)
