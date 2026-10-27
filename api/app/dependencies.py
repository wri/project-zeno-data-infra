import logging
from functools import lru_cache

from fastapi import Header
from pyiceberg.catalog import Catalog, load_catalog

from app.domain.models.environment import Environment
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.repositories.aoi_name_index import AoiNameIndex
from app.domain.repositories.aoi_type_routing_geometry_repository import (
    AoiTypeRoutingGeometryRepository,
)
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository
from app.infrastructure.persistence.duck_db_aoi_name_index import (
    DuckDbAoiNameIndex,
    NameSource,
)
from app.infrastructure.persistence.geoparquet_aoi_geometry_repository import (
    GeoParquetAoiGeometryRepository,
)
from app.infrastructure.persistence.iceberg_custom_aoi_repository import (
    IcebergCustomAoiRepository,
)


async def get_environment(
    x_environment: Environment | None = Header(include_in_schema=False, default=None),
) -> Environment:
    """Resolve the data environment from the x-environment request header.

    Defaults to production when the header is absent, so existing clients
    require no changes. Non-production environments will later require a
    bearer token; that gate is not enforced here yet.
    """
    resolved_environment = (
        x_environment if x_environment is not None else Environment.production
    )
    logging.info(
        {
            "event": "get_environment_called",
            "specified_environment": repr(x_environment),
            "resolved_environment": resolved_environment,
        }
    )

    return resolved_environment


def aoi_uris() -> dict[str, str]:
    """Each predefined AOI type's GeoParquet from the pipeline, at a pinned
    version."""
    vectors = "s3://lcl-analytics/vectors/aois"
    return {
        "admin": f"{vectors}/gadm/v4.1.85/aois.parquet",
        "key_biodiversity_area": f"{vectors}/kba/v20240904/aois.parquet",
        "protected_area": f"{vectors}/wdpa/v202512/aois.parquet",
        "indigenous_land": f"{vectors}/landmark/v20260617/aois.parquet",
    }


def get_aoi_geometry_repository() -> AoiGeometryRepository:
    """Geometries for predefined AOIs, from the pipeline's GeoParquet copy of
    each dataset at a pinned version."""
    repositories: dict[str, AoiGeometryRepository] = {}
    for aoi_type, uri in aoi_uris().items():
        repositories[aoi_type] = GeoParquetAoiGeometryRepository(uri)
    return AoiTypeRoutingGeometryRepository(repositories)


@lru_cache(maxsize=1)
def get_aoi_name_index() -> AoiNameIndex:
    """Names of every predefined AOI, loaded once per process.

    Importance and multilingual alternate names come from Wikipedia, for AOIs
    linked to Wikidata. Unlinked AOIs fall back to an importance by type: admin
    areas first, then protected areas, since people are most likely to know
    those names."""
    uris = aoi_uris()
    search = "s3://lcl-analytics/vectors/aois/search/prototype"
    return DuckDbAoiNameIndex(
        {
            "admin": NameSource(
                uri=uris["admin"],
                columns=(
                    "adm_level",
                    "name_0",
                    "name_1",
                    "name_2",
                    "varname_1",
                    "varname_2",
                    "nl_name_1",
                    "nl_name_2",
                    "is_disputed",
                ),
                name=(
                    "CASE adm_level WHEN 0 THEN name_0 WHEN 1 THEN name_1"
                    " ELSE name_2 END"
                ),
                display_name=(
                    "CASE adm_level WHEN 0 THEN name_0"
                    " WHEN 1 THEN concat_ws(', ', name_1, name_0)"
                    " ELSE concat_ws(', ', name_2, name_1, name_0) END"
                ),
                # GADM separates variant names with "|" and writes "NA" for none
                alternate_names=(
                    "list_filter(CASE adm_level"
                    " WHEN 1 THEN list_concat("
                    "string_split(coalesce(varname_1, ''), '|'), [nl_name_1])"
                    " WHEN 2 THEN list_concat("
                    "string_split(coalesce(varname_2, ''), '|'), [nl_name_2])"
                    " ELSE [] END, alternate_name -> alternate_name <> 'NA')"
                ),
                fallback_importance=(
                    "CASE adm_level WHEN 0 THEN 0.5 WHEN 1 THEN 0.45 ELSE 0.35 END"
                ),
                excluded="is_disputed",
            ),
            "key_biodiversity_area": NameSource(
                uri=uris["key_biodiversity_area"],
                columns=("natname", "intname", "country"),
                name="natname",
                display_name="concat_ws(', ', natname, country)",
                alternate_names="[intname]",
                fallback_importance="0.1",
            ),
            "protected_area": NameSource(
                uri=uris["protected_area"],
                columns=("name", "name_eng", "desig_eng", "iso3"),
                name="name",
                display_name="concat_ws(', ', name, desig_eng, iso3)",
                alternate_names="[name_eng]",
                fallback_importance="0.15",
            ),
            "indigenous_land": NameSource(
                uri=uris["indigenous_land"],
                columns=("name", "ethncty_1", "category", "country"),
                name="name",
                display_name=(
                    "concat_ws(', ', name, nullif(ethncty_1, ''), category, country)"
                ),
                alternate_names="[ethncty_1]",
                fallback_importance="0.1",
            ),
        },
        importance_uri=f"{search}/importance.parquet",
        alternate_names_uri=f"{search}/alternate_names.parquet",
    )


@lru_cache
def get_custom_aoi_catalog() -> Catalog:
    """The Iceberg catalog holding custom AOIs, configured through
    PYICEBERG_CATALOG__AOIS__* environment variables."""
    return load_catalog("aois")


@lru_cache
def get_custom_aoi_repository() -> CustomAoiRepository:
    """One repository per process, so its cached tables are reused across
    requests."""
    return IcebergCustomAoiRepository(
        get_custom_aoi_catalog(), "custom.custom_aois", "custom.custom_aoi_cells"
    )
