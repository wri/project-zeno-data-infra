from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse
from pydantic import ValidationError
from shapely.geometry import mapping

from app.authentication import require_resource_watch_admin
from app.dependencies import get_aoi_geometry_repository
from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    AoiNotFoundError,
)
from app.models.aois import AoiFeature, AoiFeatureResponse, AoiType, aoi_by_type

router = APIRouter(
    prefix="/v0/aois",
    tags=["AOIs"],
    dependencies=[Depends(require_resource_watch_admin)],
)


@router.get(
    "/{aoi_type}/{aoi_id}",
    response_class=ORJSONResponse,
    response_model=AoiFeatureResponse,
    summary="Get an AOI's geometry",
)
async def get_aoi(
    aoi_type: AoiType,
    aoi_id: str,
    geometry_repository: AoiGeometryRepository = Depends(get_aoi_geometry_repository),
):
    try:
        aoi = aoi_by_type.validate_python({"type": aoi_type, "ids": [aoi_id]})
    except ValidationError:
        raise HTTPException(status_code=422, detail=f"Invalid {aoi_type} id.")
    try:
        geometries, areas_ha = await geometry_repository.load(aoi)
    except AoiNotFoundError:
        raise HTTPException(status_code=404, detail=f"No {aoi_type} AOI {aoi_id}.")
    return AoiFeatureResponse(
        data=AoiFeature(
            id=aoi_id,
            geometry=mapping(geometries[0]),
            properties={"aoi_type": aoi_type, "area_ha": areas_ha[0]},
        )
    )
