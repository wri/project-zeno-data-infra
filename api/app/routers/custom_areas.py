from fastapi import APIRouter, Depends, HTTPException, Response

from app.authentication import require_resource_watch_admin
from app.dependencies import get_custom_aoi_repository
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository
from app.models.custom_areas import CustomAreasIn
from app.use_cases.custom_areas.custom_area_service import (
    CustomAreaService,
    InvalidCustomAreaError,
)

router = APIRouter(
    prefix="/v0/custom_areas",
    dependencies=[Depends(require_resource_watch_admin)],
    include_in_schema=False,
)


def get_custom_area_service(
    repository: CustomAoiRepository = Depends(get_custom_aoi_repository),
) -> CustomAreaService:
    return CustomAreaService(repository)


@router.put("")
async def save_custom_areas(
    custom_areas: CustomAreasIn,
    service: CustomAreaService = Depends(get_custom_area_service),
):
    try:
        await service.save({f.id: f.geometry for f in custom_areas.features})
    except InvalidCustomAreaError as error:
        raise HTTPException(status_code=422, detail=str(error))
    return {"data": {"ids": [f.id for f in custom_areas.features]}, "status": "success"}


@router.delete("/{aoi_id}", status_code=204)
async def delete_custom_area(
    aoi_id: str, service: CustomAreaService = Depends(get_custom_area_service)
):
    await service.delete(aoi_id)
    return Response(status_code=204)
