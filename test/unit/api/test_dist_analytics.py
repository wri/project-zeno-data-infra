from api.app.query import create_gadm_dist_query
import re

def test_create_gadm_adm2_dist_query_no_intersection():
    query = create_gadm_dist_query(["IDN", 24, 9], [])
    
    assert query == strip_extra_whitespace("""
        SELECT country, region, subregion, alert_date, alert_confidence AS confidence, SUM(count) AS value
        FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts.parquet'
        WHERE country = 'IDN' AND region = 24 AND subregion = 9
        GROUP BY country, region, subregion, alert_date, alert_confidence
        ORDER BY country, region, subregion, alert_date, alert_confidence
    """)


def test_create_gadm_adm1_dist_query_no_intersection():
    query = create_gadm_dist_query(["IDN", 24], [])
    
    assert query == strip_extra_whitespace("""
        SELECT country, region, alert_date, alert_confidence AS confidence, SUM(count) AS value
        FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts.parquet'
        WHERE country = 'IDN' AND region = 24
        GROUP BY country, region, alert_date, alert_confidence
        ORDER BY country, region, alert_date, alert_confidence
    """)


def test_create_gadm_iso_dist_query_no_intersection():
    query = create_gadm_dist_query(["IDN"], [])
    
    assert query == strip_extra_whitespace("""
        SELECT country, alert_date, alert_confidence AS confidence, SUM(count) AS value
        FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts.parquet'
        WHERE country = 'IDN'
        GROUP BY country, alert_date, alert_confidence
        ORDER BY country, alert_date, alert_confidence
    """)

def test_create_gadm_adm2_dist_query_natural_lands_intersection():
    query = create_gadm_dist_query(["IDN", 24, 9], ["natural_lands"])
    
    assert query == strip_extra_whitespace("""
        SELECT country, region, subregion, natural_land_class, alert_date, alert_confidence AS confidence, SUM(count) AS value
        FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts_by_natural_lands.parquet'
        WHERE country = 'IDN' AND region = 24 AND subregion = 9
        GROUP BY country, region, subregion, natural_land_class, alert_date, alert_confidence
        ORDER BY country, region, subregion, natural_land_class, alert_date, alert_confidence
    """)


def test_create_gadm_adm2_dist_query_drivers_intersection():
    query = create_gadm_dist_query(["IDN", 24, 9], ["driver"])
    
    assert query == strip_extra_whitespace("""
        SELECT country, region, subregion, ldacs_driver, alert_date, alert_confidence AS confidence, SUM(count) AS value
        FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts_by_driver.parquet'
        WHERE country = 'IDN' AND region = 24 AND subregion = 9
        GROUP BY country, region, subregion, ldacs_driver, alert_date, alert_confidence
        ORDER BY country, region, subregion, ldacs_driver, alert_date, alert_confidence
    """)


def strip_extra_whitespace(string: str) -> str:
    return re.sub(r'\s+', ' ', string).strip()
