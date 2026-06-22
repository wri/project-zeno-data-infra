from typing import Tuple


def create_gadm_integrated_alerts_query(
    gadm_id: Tuple[str, int, int], table: str
) -> str:
    # Build up the DuckDB query based on GADM ID. The precomputed parquet stores
    # the alert date/confidence as intdist_alert_date/intdist_alert_confidence;
    # alias them to the public alert_date/alert_confidence schema.
    from_clause = f"FROM '{table}'"
    select_clause = "SELECT country"
    where_clause = f"WHERE country = '{gadm_id[0]}'"
    by_clause = "BY country"

    # Includes region, so add relevant filters, selects and group bys
    if len(gadm_id) > 1:
        select_clause += ", region"
        where_clause += f" AND region = {gadm_id[1]}"
        by_clause += ", region"

    # Includes subregion, so add relevant filters, selects and group bys
    if len(gadm_id) > 2:
        select_clause += ", subregion"
        where_clause += f" AND subregion = {gadm_id[2]}"
        by_clause += ", subregion"

    by_clause += ", intdist_alert_date, intdist_alert_confidence"
    group_by_clause = f"GROUP {by_clause}"
    order_by_clause = f"ORDER {by_clause}"

    select_clause += (
        ", STRFTIME(intdist_alert_date, '%Y-%m-%d') AS alert_date"
        ", intdist_alert_confidence AS alert_confidence"
        ", SUM(area_ha)::FLOAT AS area_ha"
    )
    query = (
        f"{select_clause} {from_clause} {where_clause} "
        f"{group_by_clause} {order_by_clause}"
    )

    return query
