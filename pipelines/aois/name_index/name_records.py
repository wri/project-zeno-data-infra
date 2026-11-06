from dataclasses import dataclass
from typing import Mapping, Sequence

import duckdb
import pyarrow as pa


@dataclass(frozen=True)
class NameSpec:
    """How one AOI type is named, as SQL over the attributes of its pipeline
    source's AOIs: its name, display name, list of alternate names, importance
    when Wikipedia gives none (0 to 1), and whether to leave it out of search."""

    aoi_type: str
    source: str
    name: str
    display_name: str
    fallback_importance: str
    alternate_names: str = "[]"
    excluded: str = "false"


def name_records(
    aois: Mapping[str, pa.Table],
    specs: Sequence[NameSpec],
    importance: pa.Table,
    alternate_names: pa.Table,
) -> pa.Table:
    """One record per named AOI: its name, display name, alternate names from
    both its source and Wikipedia, importance and area."""
    connection = duckdb.connect()
    named = pa.concat_tables(
        [own_names(connection, spec, aois[spec.aoi_type]) for spec in specs]
    )
    connection.register("named", named)
    connection.register("wikipedia_importance", importance)
    connection.register("wikipedia_names", alternate_names)
    return connection.sql(
        """
        WITH wikipedia_alternates AS (
            SELECT aoi_type, aoi_id, list(alternate_name) AS alternate_names
            FROM wikipedia_names
            GROUP BY aoi_type, aoi_id
        ),
        all_alternates AS (
            SELECT aoi_type, aoi_id,
                list_transform(
                    list_concat(
                        named.alternate_names,
                        coalesce(wikipedia_alternates.alternate_names, [])
                    ),
                    alternate_name -> trim(alternate_name)
                ) AS alternate_names
            FROM named
            LEFT JOIN wikipedia_alternates USING (aoi_type, aoi_id)
        )
        SELECT aoi_type, aoi_id, name, display_name,
            list_distinct(list_filter(
                all_alternates.alternate_names,
                alternate_name -> alternate_name <> ''
            )) AS alternate_names,
            coalesce(wikipedia_importance.importance, fallback_importance)
                AS importance,
            area_ha
        FROM named
        JOIN all_alternates USING (aoi_type, aoi_id)
        LEFT JOIN wikipedia_importance USING (aoi_type, aoi_id)
        """
    ).to_arrow_table()


def own_names(
    connection: duckdb.DuckDBPyConnection, spec: NameSpec, aois: pa.Table
) -> pa.Table:
    """Each AOI's names as its spec gives them, leaving out excluded and
    unnamed AOIs."""
    connection.register("aois", aois)
    names = connection.sql(
        f"""
        SELECT $aoi_type AS aoi_type, aoi_id, {spec.name} AS name,
            {spec.display_name} AS display_name,
            {spec.alternate_names}::VARCHAR[] AS alternate_names,
            ({spec.fallback_importance})::DOUBLE AS fallback_importance,
            area_ha::DOUBLE AS area_ha
        FROM aois
        WHERE NOT coalesce({spec.excluded}, false) AND {spec.name} IS NOT NULL
        """,
        params={"aoi_type": spec.aoi_type},
    ).to_arrow_table()
    connection.unregister("aois")
    return names
