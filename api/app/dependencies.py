import logging

from fastapi import Header

from app.domain.models.environment import Environment
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.repositories.aoi_type_routing_geometry_repository import (
    AoiTypeRoutingGeometryRepository,
)
from app.domain.repositories.fallback_aoi_geometry_repository import (
    FallbackAoiGeometryRepository,
)
from app.infrastructure.external_services.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.infrastructure.persistence.geoparquet_aoi_geometry_repository import (
    GeoParquetAoiGeometryRepository,
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
    each dataset at a pinned version. Until a dataset's GeoParquet exists, its
    geometries come from the Data API instead."""
    vectors = "s3://lcl-analytics/vectors/aois"
    data_api = DataApiAoiGeometryRepository()

    def with_data_api_fallback(uri: str) -> AoiGeometryRepository:
        return FallbackAoiGeometryRepository(
            GeoParquetAoiGeometryRepository(uri), data_api
        )

    return AoiTypeRoutingGeometryRepository(
        {
            "admin": GeoParquetAoiGeometryRepository(
                f"{vectors}/gadm/v4.1.85/aois.parquet"
            ),
            "key_biodiversity_area": with_data_api_fallback(
                f"{vectors}/kba/v20250911/aois.parquet"
            ),
            "protected_area": with_data_api_fallback(
                f"{vectors}/wdpa/v202512/aois.parquet"
            ),
            "indigenous_land": with_data_api_fallback(
                f"{vectors}/landmark/v20260617/aois.parquet"
            ),
        }
    )
