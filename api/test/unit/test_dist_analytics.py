import re

import pytest

from app.analysis.common.analysis import (
    get_geojson_request_for_data_api,
    get_geojsons_from_data_api,
    get_sql_in_list,
)


def strip_extra_whitespace(string: str) -> str:
    return re.sub(r"\s+", " ", string).strip()


@pytest.mark.asyncio
async def test_get_geojsons_from_data_api():
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
    geojson = await get_geojsons_from_data_api(
        aoi, send_request=send_request_to_data_api_test
    )
    assert geojson[0]["type"] == "MultiPolygon"


@pytest.mark.asyncio
async def test_get_geojsons_from_data_api_failed():
    async def send_request_to_data_api_test_failed(url, params):
        return {"detail": "you suck", "status": "failed"}

    aoi = {"type": "protected_area", "ids": ["555625448"]}
    try:
        await get_geojsons_from_data_api(
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
        "sql": "select gfw_geojson from data where wdpa_pid in ('555625448') order by wdpa_pid"
    }


def test_get_geojson_request_for_data_api_indigenous_lands():
    aoi = {"type": "indigenous_land", "ids": ["CAN1"]}
    url, params = get_geojson_request_for_data_api(aoi)
    assert (
        url
        == "https://data-api.globalforestwatch.org/dataset/landmark_ip_lc_and_indicative_poly/latest/query"
    )
    assert params == {
        "sql": "select gfw_geojson from data where landmark_id in ('CAN1') order by landmark_id"
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
