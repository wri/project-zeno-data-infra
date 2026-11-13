from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import ORJSONResponse

from app.dependencies import get_aoi_geometry_repository
from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    AoiNotFoundError,
)
from app.domain.services.deforestation_forest import DeforestationForest
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.deforestation import DeforestationAnalyticsIn
from app.routers.common_analytics import create_analysis
from app.routers.land_change.tree_cover_loss.tree_cover_loss import (
    _datamart_resource_link_response,
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from app.use_cases.analysis.deforestation_request import DeforestationRequest

router = APIRouter(prefix="/deforestation")


def get_deforestation_request(
    geometries: AoiGeometryRepository = Depends(get_aoi_geometry_repository),
) -> DeforestationRequest:
    # Humid tropical primary forest is mapped between 30°S and 30°N.
    return DeforestationRequest(geometries, DeforestationForest(tropics_latitude=30))


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
    summary="Create Deforestation Analysis Task",
    description=(
        "Tree cover loss in natural forest, with the forest chosen from the "
        "area's location: humid tropical primary forest when every area is "
        "centred within 30°S-30°N, intact forest landscapes otherwise. The "
        "result is a tree cover loss analysis whose metadata names the forest "
        "used."
    ),
    openapi_extra={
        "x-dataset": {
            "measures": "Area of tree cover loss (ha) and resulting carbon emissions",
            "time_basis": "annual",
            "coverage": "2002-2025",
            "baseline": (
                "Primary forest (2001) in the tropics, intact forest landscapes "
                "elsewhere, at 30% canopy cover"
            ),
            "prefer_for": (
                "Questions about deforestation that don't name a forest; use "
                "tree_cover_loss for forest or tree cover loss"
            ),
        }
    },
)
async def create(
    *,
    data: DeforestationAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
    deforestation: DeforestationRequest = Depends(get_deforestation_request),
    service: AnalysisService = Depends(create_analysis_service),
):
    try:
        loss = await deforestation.tree_cover_loss_request(data)
    except AoiNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error))
    return await create_analysis(
        data=loss,
        service=service,
        request=request,
        background_tasks=background_tasks,
        resource_link_callback=_datamart_resource_link_response,
    )
