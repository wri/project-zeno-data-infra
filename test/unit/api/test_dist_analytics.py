from api.app.query import create_gadm_dist_query
import re

def test_create_gadm_adm2_dist_query_no_intersection():
    query = create_gadm_dist_query(("IDN", 24, 9), [])
    
    assert query == re.sub(r'\s+', ' ', """
        SELECT country, region, subregion, alert_date, alert_confidence AS confidence, SUM(count) AS value
        FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/gadm_dist_alerts.parquet'
        WHERE country = 'IDN' AND region = 24 AND subregion = 9
        GROUP BY country, region, subregion, alert_date, alert_confidence
        ORDER BY country, region, subregion, alert_date, alert_confidence
    """).strip()