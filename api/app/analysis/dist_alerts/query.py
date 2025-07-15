from typing import List, Tuple


def create_gadm_dist_query(
    gadm_id: Tuple[str, int, int], intersections: List[str]
) -> str:
    # Each intersection will be in a different parquet file
    if not intersections:
        table = "gadm_dist_alerts"
    elif intersections[0] == "driver":
        table = "gadm_dist_alerts_by_driver"
        intersection_col = "ldacs_driver"
    elif intersections[0] == "natural_lands":
        table = "gadm_dist_alerts_by_natural_lands"
        intersection_col = "natural_lands_class"
    else:
        raise ValueError(f"No way to calculate intersection {intersections[0]}")

    # TODO use some better pattern here is so it doesn't become spaghetti once we have more datasets. ORM?
    # TODO use final pipeline locations and schema for parquet files
    # TODO this should be done in a background task and written to file
    # Build up the DuckDB query based on GADM ID and intersection
    from_clause = (
        f"FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/{table}.parquet'"
    )
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

    # Includes an intersection, so group by the appropriate column
    if intersections:
        select_clause += f", {intersection_col}"
        by_clause += f", {intersection_col}"

    by_clause += ", alert_date, alert_confidence"
    group_by_clause = f"GROUP {by_clause}"
    order_by_clause = f"ORDER {by_clause}"

    # Query and make sure output names match the expected schema (?)
    select_clause += ", STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, SUM(count)::INT AS value"
    query = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"

    return query
