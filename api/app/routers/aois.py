from dataclasses import asdict
from typing import Callable, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse
from pydantic import ValidationError
from shapely.geometry import mapping

from app.authentication import require_resource_watch_admin
from app.dependencies import (
    get_aoi_geometry_repository,
    get_aoi_name_search,
    get_custom_aoi_repository,
)
from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    AoiNotFoundError,
)
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository
from app.models.aois import (
    AoiFeature,
    AoiFeatureResponse,
    AoiMatchOut,
    AoiSearchResponse,
    AoiType,
    aoi_by_type,
)
from app.use_cases.aois.aoi_name_search import AoiNameSearch

router = APIRouter(
    prefix="/v0/aois",
    tags=["AOIs"],
    dependencies=[Depends(require_resource_watch_admin)],
)


def get_custom_aoi_repository_factory() -> Callable[[], CustomAoiRepository]:
    """The custom AOI store, created only when a search includes a user's AOIs."""
    return get_custom_aoi_repository


@router.get(
    "/search",
    response_class=ORJSONResponse,
    response_model=AoiSearchResponse,
    summary="Search AOIs by name",
    description=(
        "AOIs whose names best match `q`, best first, ignoring case and "
        "accents. With `user_id`, that user's custom AOIs are included and "
        "ranked above other AOIs that match as well. Use the returned type and "
        "id to request an AOI's geometry or analyses."
    ),
)
async def search_aois(
    q: str = Query(..., min_length=1, max_length=100),
    type: Optional[List[AoiType]] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    user_id: Optional[str] = Query(None, min_length=1),
    name_search: AoiNameSearch = Depends(get_aoi_name_search),
    custom_aoi_repository: Callable[[], CustomAoiRepository] = Depends(
        get_custom_aoi_repository_factory
    ),
):
    custom_aois = (
        await custom_aoi_repository().names_owned_by(user_id) if user_id else []
    )
    matches = await name_search.search(
        q, aoi_types=type, limit=limit, custom_aois=custom_aois
    )
    return AoiSearchResponse(data=[AoiMatchOut(**asdict(m)) for m in matches])


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
