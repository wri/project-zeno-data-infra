import re

import pytest

from app.analysis.common.analysis import get_sql_in_list
from app.analysis.dist_alerts.query import create_gadm_dist_query


class TestGadmQueryAdm2NoIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(["IDN", "24", "9"], "gadm_dist_alerts")

    def test_create_gadm_adm2_dist_query_no_intersection_select_clause(self):
        expected_clause = "SELECT country, region, subregion, STRFTIME(dist_alert_date, '%Y-%m-%d') AS dist_alert_date, dist_alert_confidence, SUM(area_ha)::FLOAT AS area_ha"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_from_clause(self):
        expected_clause = "gadm_dist_alerts'"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_group_by_clause(self):
        expected_clause = "GROUP BY country, region, subregion, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_no_intersection_order_by_clause(self):
        expected_clause = "ORDER BY country, region, subregion, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query


class TestGadmQueryAdm1NoIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(["IDN", "24"], "gadm_dist_alerts")

    def test_create_gadm_adm1_dist_query_no_intersection_select_clause(self):
        expected_clause = "SELECT country, region, STRFTIME(dist_alert_date, '%Y-%m-%d') AS dist_alert_date, dist_alert_confidence, SUM(area_ha)::FLOAT AS area_ha"
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_from_clause(self):
        expected_clause = "gadm_dist_alerts'"
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24"
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_group_by_clause(self):
        expected_clause = (
            "GROUP BY country, region, dist_alert_date, dist_alert_confidence"
        )
        assert expected_clause in self.query

    def test_create_gadm_adm1_dist_query_no_intersection_order_by_clause(self):
        expected_clause = (
            "ORDER BY country, region, dist_alert_date, dist_alert_confidence"
        )
        assert expected_clause in self.query


class TestGadmQueryIsoNoIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(["IDN"], "gadm_dist_alerts")

    def test_create_gadm_iso_dist_query_no_intersection_select_clause(self):
        expected_clause = "SELECT country, STRFTIME(dist_alert_date, '%Y-%m-%d') AS dist_alert_date, dist_alert_confidence, SUM(area_ha)::FLOAT AS area_ha"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_from_clause(self):
        expected_clause = "gadm_dist_alerts'"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN'"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_group_by_clause(self):
        expected_clause = "GROUP BY country, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_iso_dist_query_no_intersection_order_by_clause(self):
        expected_clause = "ORDER BY country, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query


class TestGadmQueryAdm2NaturalLandsIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(
            ["IDN", "24", "9"], "gadm_dist_alerts_by_natural_lands", "natural_lands"
        )

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_select_clause(self):
        expected_clause = "SELECT country, region, subregion, natural_land_class, STRFTIME(dist_alert_date, '%Y-%m-%d') AS dist_alert_date, dist_alert_confidence, SUM(area_ha)::FLOAT AS area_ha"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_from_clause(self):
        expected_clause = "gadm_dist_alerts_by_natural_lands'"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_group_by_clause(
        self,
    ):
        expected_clause = "GROUP BY country, region, subregion, natural_land_class, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_natural_lands_intersection_order_by_clause(
        self,
    ):
        expected_clause = "ORDER BY country, region, subregion, natural_land_class, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query


class TestGadmQueryAdm2DriverIntersections:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        self.query = create_gadm_dist_query(
            ["IDN", "24", "9"], "gadm_dist_alerts_by_driver", "driver"
        )

    def test_create_gadm_adm2_dist_query_drivers_intersection_select_clause(self):
        expected_clause = "SELECT country, region, subregion, driver, STRFTIME(dist_alert_date, '%Y-%m-%d') AS dist_alert_date, dist_alert_confidence, SUM(area_ha)::FLOAT AS area_ha"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_from_clause(self):
        expected_clause = "gadm_dist_alerts_by_driver'"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_where_clause(self):
        expected_clause = "WHERE country = 'IDN' AND region = 24 AND subregion = 9"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_group_by_clause(self):
        expected_clause = "GROUP BY country, region, subregion, driver, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query

    def test_create_gadm_adm2_dist_query_drivers_intersection_order_by_clause(self):
        expected_clause = "ORDER BY country, region, subregion, driver, dist_alert_date, dist_alert_confidence"
        assert expected_clause in self.query


def strip_extra_whitespace(string: str) -> str:
    return re.sub(r"\s+", " ", string).strip()


def test_sql_in_list():
    assert get_sql_in_list(["1", "2", "3"]) == "('1', '2', '3')"
