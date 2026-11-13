import logging
from functools import lru_cache

from fastapi import Header
from pyiceberg.catalog import Catalog, load_catalog

from app.domain.models.environment import Environment
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.repositories.aoi_subregions import IntersectingSubregions
from app.domain.repositories.aoi_type_routing_geometry_repository import (
    AoiTypeRoutingGeometryRepository,
)
from app.domain.services.aoi_name_ranking import AoiNameRanking
from app.infrastructure.persistence.geoparquet_admin_subregions import (
    GeoParquetAdminSubregions,
)
from app.infrastructure.persistence.geoparquet_aoi_geometry_repository import (
    GeoParquetAoiGeometryRepository,
)
from app.infrastructure.persistence.geoparquet_intersecting_subregions import (
    GeoParquetIntersectingSubregions,
)
from app.infrastructure.persistence.iceberg_custom_aoi_repository import (
    IcebergCustomAoiRepository,
)
from app.infrastructure.persistence.tantivy_aoi_name_candidates import (
    TantivyAoiNameCandidates,
)
from app.use_cases.aois.aoi_name_search import AoiNameSearch
from app.use_cases.aois.list_subregions import ListSubregions


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


def predefined_aoi_uris() -> dict[str, str]:
    """The pipeline's GeoParquet copy of each predefined AOI dataset, at a
    pinned version."""
    vectors = "s3://lcl-analytics/vectors/aois"
    return {
        "admin": f"{vectors}/gadm/v4.1.85/aois.parquet",
        "key_biodiversity_area": f"{vectors}/kba/v20240904/aois.parquet",
        "protected_area": f"{vectors}/wdpa/v202512/aois.parquet",
        "indigenous_land": f"{vectors}/landmark/v20260617/aois.parquet",
    }


def get_aoi_geometry_repository() -> AoiGeometryRepository:
    """Geometries for predefined AOIs and for stored custom AOIs."""
    repositories: dict[str, AoiGeometryRepository] = {}
    for aoi_type, uri in predefined_aoi_uris().items():
        repositories[aoi_type] = GeoParquetAoiGeometryRepository(uri)
    repositories["custom_aoi"] = get_custom_aoi_repository()
    return AoiTypeRoutingGeometryRepository(repositories)


@lru_cache(maxsize=1)
def get_country_ids() -> tuple[str, ...]:
    """Every country's admin id, read on first use and kept for the process."""
    countries = GeoParquetAdminSubregions(predefined_aoi_uris()["admin"]).read(None, 0)
    return tuple(country.aoi_id for country in countries)


def get_list_subregions() -> ListSubregions:
    """Lists predefined AOIs within an AOI. Name columns differ per dataset."""
    uris = predefined_aoi_uris()
    name_columns = {
        "key_biodiversity_area": "intname",
        "protected_area": "name",
        "indigenous_land": "name",
    }
    intersecting: dict[str, IntersectingSubregions] = {}
    for aoi_type, name_column in name_columns.items():
        intersecting[aoi_type] = GeoParquetIntersectingSubregions(
            uris[aoi_type], aoi_type, name_column
        )
    return ListSubregions(
        admin=GeoParquetAdminSubregions(uris["admin"]),
        intersecting=intersecting,
        geometries=get_aoi_geometry_repository(),
    )


@lru_cache(maxsize=1)
def get_aoi_name_search() -> AoiNameSearch:
    """Searches the names of every predefined AOI, in the index the AOI pipeline
    built on a pinned date, loaded once per process."""
    return AoiNameSearch(
        TantivyAoiNameCandidates("s3://lcl-analytics/vectors/aois/search/v20261105"),
        AoiNameRanking(),
    )


@lru_cache
def get_custom_aoi_catalog() -> Catalog:
    """The Iceberg catalog holding custom AOIs, configured through
    PYICEBERG_CATALOG__AOIS__* environment variables."""
    return load_catalog("aois")


@lru_cache
def get_custom_aoi_repository() -> IcebergCustomAoiRepository:
    """One repository per process, so its cached tables are reused across
    requests."""
    return IcebergCustomAoiRepository(
        get_custom_aoi_catalog, "custom.custom_aois", "custom.custom_aoi_cells"
    )
