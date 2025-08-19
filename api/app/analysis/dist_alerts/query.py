from typing import Optional, Tuple


def create_gadm_dist_query(
    gadm_id: Tuple[str, int, int], table: str, intersection: Optional[str] = None
) -> str:
    # TODO use some better pattern here is so it doesn't become spaghetti once we have more datasets. ORM?
    # TODO use final pipeline locations and schema for parquet files
    # TODO this should be done in a background task and written to file
    # Build up the DuckDB query based on GADM ID and intersection

    intersection_col = None
    if intersection is not None:
        if intersection == "driver":
            intersection_col = "ldacs_driver"
        elif intersection == "natural_lands":
            intersection_col = "natural_land_class"
        elif intersection == "grasslands":
            intersection_col = "grasslands"
        elif intersection == "land_cover":
            intersection_col = "land_cover"

    from_clause = f"FROM '/tmp/{table}.parquet'"
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
    if intersection:
        select_clause += f", {intersection_col}"
        by_clause += f", {intersection_col}"

    by_clause += ", alert_date, alert_confidence"
    group_by_clause = f"GROUP {by_clause}"
    order_by_clause = f"ORDER {by_clause}"

    # Query and make sure output names match the expected schema (?)
    select_clause += ", STRFTIME(alert_date, '%Y-%m-%d') AS alert_date, alert_confidence AS confidence, "
    # This is temporary - we will convert all queries to area__ha soon.
    if intersection == "land_cover":
        select_clause += "SUM(area__ha)::FLOAT AS value"
    else:
        select_clause += "SUM(count)::INT AS value"
    query = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"

    return query
