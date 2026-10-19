import logging
from functools import lru_cache

from fastapi import Header
from pyiceberg.catalog import Catalog, load_catalog

from app.domain.models.environment import Environment
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.repositories.aoi_type_routing_geometry_repository import (
    AoiTypeRoutingGeometryRepository,
)
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository
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


def get_aoi_geometry_repository() -> AoiGeometryRepository:
    """Geometries for predefined AOIs, from the pipeline's GeoParquet copy of
    each dataset at a pinned version."""
    vectors = "s3://lcl-analytics/vectors/aois"
    return AoiTypeRoutingGeometryRepository(
        {
            "admin": GeoParquetAoiGeometryRepository(
                f"{vectors}/gadm/v4.1.85/aois.parquet"
            ),
            "key_biodiversity_area": GeoParquetAoiGeometryRepository(
                f"{vectors}/kba/v20250911/aois.parquet"
            ),
            "protected_area": GeoParquetAoiGeometryRepository(
                f"{vectors}/wdpa/v202512/aois.parquet"
            ),
            "indigenous_land": GeoParquetAoiGeometryRepository(
                f"{vectors}/landmark/v20260617/aois.parquet"
            ),
        }
    )


@lru_cache
def get_custom_aoi_catalog() -> Catalog:
    """The Iceberg catalog holding custom AOIs, configured through
    PYICEBERG_CATALOG__AOIS__* environment variables."""
    return load_catalog("aois")


def get_custom_aoi_repository() -> CustomAoiRepository:
    return IcebergCustomAoiRepository(get_custom_aoi_catalog(), "custom.custom_aois")
