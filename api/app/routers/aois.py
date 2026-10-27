from dataclasses import asdict
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse
from pydantic import ValidationError
from shapely.geometry import mapping

from app.dependencies import get_aoi_geometry_repository, get_aoi_name_index
from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    AoiNotFoundError,
)
from app.domain.repositories.aoi_name_index import AoiNameIndex
from app.models.aois import (
    AoiFeature,
    AoiFeatureResponse,
    AoiMatchOut,
    AoiSearchResponse,
    PredefinedAoiType,
    predefined_aoi,
)

router = APIRouter(prefix="/v0/aois", tags=["AOIs"])


@router.get(
    "/search",
    response_class=ORJSONResponse,
    response_model=AoiSearchResponse,
    summary="Search AOIs by name",
    description=(
        "Predefined AOIs whose names best match `q`, best first, ignoring case "
        "and accents. Use the returned type and id to request an AOI's geometry "
        "or analyses."
    ),
)
async def search_aois(
    q: str = Query(..., min_length=1, max_length=200),
    type: Optional[List[PredefinedAoiType]] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    name_index: AoiNameIndex = Depends(get_aoi_name_index),
):
    matches = await name_index.search(q, aoi_types=type, limit=limit)
    return AoiSearchResponse(data=[AoiMatchOut(**asdict(m)) for m in matches])


@router.get(
    "/{aoi_type}/{aoi_id}",
    response_class=ORJSONResponse,
    response_model=AoiFeatureResponse,
    summary="Get an AOI's geometry",
)
async def get_aoi(
    aoi_type: PredefinedAoiType,
    aoi_id: str,
    geometry_repository: AoiGeometryRepository = Depends(get_aoi_geometry_repository),
):
    try:
        aoi = predefined_aoi.validate_python({"type": aoi_type, "ids": [aoi_id]})
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
