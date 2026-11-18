from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import ORJSONResponse

from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.deforestation import (
    EudrSupplyChainDeforestationAnalyticsIn,
)
from app.routers.common_analytics import create_analysis
from app.routers.land_change.tree_cover_loss.tree_cover_loss import (
    _datamart_resource_link_response,
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService

router = APIRouter(prefix="/eudr_supply_chain_deforestation")


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
    summary="Create EUDR Supply Chain Deforestation Analysis Task",
    description=(
        "Tree cover loss in SBTN natural forest, as mapped in 2020, at 10% "
        "canopy cover: the forest EUDR, supply chain and deforestation-free "
        "commitments refer to. Includes secondary natural forest and excludes "
        "plantations. The result is a tree cover loss analysis."
    ),
    openapi_extra={
        "x-dataset": {
            "measures": "Area of tree cover loss (ha) and resulting carbon emissions",
            "time_basis": "annual",
            "coverage": "2021-2025",
            "baseline": "SBTN natural forest in 2020, at 10% canopy cover",
            "prefer_for": (
                "EUDR, supply chain, deforestation-free or no-conversion questions"
            ),
        }
    },
)
async def create(
    *,
    data: EudrSupplyChainDeforestationAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
    service: AnalysisService = Depends(create_analysis_service),
):
    return await create_analysis(
        data=data.tree_cover_loss_request(),
        service=service,
        request=request,
        background_tasks=background_tasks,
        resource_link_callback=_datamart_resource_link_response,
    )
