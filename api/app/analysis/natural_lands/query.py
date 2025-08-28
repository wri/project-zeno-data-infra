from typing import Tuple


def create_gadm_natural_lands_query(gadm_id: Tuple[str, int, int], table: str) -> str:
    # Build up the DuckDB query based on GADM ID and intersection

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

    by_clause += ", natural_lands_class"
    select_clause += ", natural_lands_class"
    group_by_clause = f"GROUP {by_clause}"
    order_by_clause = f"ORDER {by_clause}"

    # Query and make sure output names match the expected schema (?)
    select_clause += ", SUM(area_ha) AS area_ha"
    query = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"

    return query
