import logging
from typing import Dict

from fastapi import Depends, Header

from app.domain.models.environment import Environment, resolve_uris
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.repositories.aoi_type_routing_geometry_repository import (
    AoiTypeRoutingGeometryRepository,
)
from app.domain.repositories.concession_aoi_geometry_repository import (
    ConcessionAoiGeometryRepository,
)
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.infrastructure.persistence.geoparquet_aoi_geometry_repository import (
    GeoParquetAoiGeometryRepository,
)

# Should match the GeoParquet outputs of the concessions flow in pipelines.
CONCESSION_GEOMETRY_URIS: Dict[Environment, Dict[str, str]] = {
    Environment.production: {
        "oil_palm": "s3://lcl-analytics/vectors/concessions/oil_palm/v2025/concessions.parquet",  # noqa: E501
    },
}


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


def get_aoi_geometry_repository(
    environment: Environment = Depends(get_environment),
) -> AoiGeometryRepository:
    data_api = DataApiAoiGeometryRepository()
    concession_uris = resolve_uris(CONCESSION_GEOMETRY_URIS, environment)
    return AoiTypeRoutingGeometryRepository(
        {
            "key_biodiversity_area": data_api,
            "protected_area": data_api,
            "indigenous_land": data_api,
            "concession": ConcessionAoiGeometryRepository(
                {
                    concession_type: GeoParquetAoiGeometryRepository(uri)
                    for concession_type, uri in concession_uris.items()
                }
            ),
        }
    )
