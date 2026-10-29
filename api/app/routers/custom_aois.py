import logging

from fastapi import APIRouter, Depends, HTTPException, Response

from app.authentication import require_resource_watch_admin
from app.dependencies import get_custom_aoi_repository
from app.domain.repositories.custom_aoi_repository import CustomAoiRepository
from app.models.common.authentication import User
from app.models.custom_aois import CustomAoisIn
from app.use_cases.custom_aois.custom_aoi_service import (
    CustomAoiService,
    InvalidCustomAoiError,
)

router = APIRouter(
    prefix="/v0/custom_aois",
    dependencies=[Depends(require_resource_watch_admin)],
    include_in_schema=False,
)


def get_custom_aoi_service(
    repository: CustomAoiRepository = Depends(get_custom_aoi_repository),
) -> CustomAoiService:
    return CustomAoiService(repository)


@router.put("")
async def save_custom_aois(
    custom_aois: CustomAoisIn,
    service: CustomAoiService = Depends(get_custom_aoi_service),
    caller: User = Depends(require_resource_watch_admin),
):
    ids = [aoi.id for aoi in custom_aois.aois]
    try:
        await service.save(custom_aois)
    except InvalidCustomAoiError as error:
        raise HTTPException(status_code=422, detail=str(error))
    logging.info({"event": "custom_aois_saved", "ids": ids, "caller": caller.id})
    return {"data": {"ids": ids}, "status": "success"}


@router.delete("/{aoi_id}", status_code=204)
async def delete_custom_aoi(
    aoi_id: str,
    service: CustomAoiService = Depends(get_custom_aoi_service),
    caller: User = Depends(require_resource_watch_admin),
):
    await service.delete(aoi_id)
    logging.info({"event": "custom_aoi_deleted", "aoi_id": aoi_id, "caller": caller.id})
    return Response(status_code=204)
