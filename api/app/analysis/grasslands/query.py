from typing import Optional, Tuple


def create_gadm_grasslands_query(gadm_id: Tuple[str, int, int], table: str) -> str:
    # TODO use some better pattern here is so it doesn't become spaghetti once we have more datasets. ORM?
    # TODO use final pipeline locations and schema for parquet files
    # TODO this should be done in a background task and written to file
    # Build up the DuckDB query based on GADM ID and intersection

    from_clause = f"FROM '/tmp/{table}.parquet'"
    select_clause = "SELECT year, country"
    where_clause = f"WHERE country = '{gadm_id[0]}'"
    by_clause = "BY year, country"

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

    group_by_clause = f"GROUP {by_clause}"
    order_by_clause = f"ORDER {by_clause}"

    # Query and make sure output names match the expected schema (?)
    select_clause += ", SUM(grassland_area) AS grassland_area"
    query = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"

    return query
