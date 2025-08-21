from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.compute_engines.compute_engine import (
    ComputeEngine,
    DuckDbPrecalcQueryService,
    FloxOTFHandler,
    PrecalcHandler,
)
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.models.common.analysis import AnalyticsOut
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.tree_cover_loss import (
    TreeCoverLossAnalytics,
    TreeCoverLossAnalyticsIn,
    TreeCoverLossAnalyticsResponse,
)
from app.routers.common_analytics import create_analysis, get_analysis
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

ANALYTICS_NAME = "tree_cover_loss"
router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service(request: Request) -> AnalysisService:
    compute_engine = ComputeEngine(
        handler=PrecalcHandler(
            precalc_query_service=DuckDbPrecalcQueryService(),
            next_handler=FloxOTFHandler(
                dataset_repository=ZarrDatasetRepository(),
                aoi_geometry_repository=DataApiAoiGeometryRepository(),
                dask_client=request.app.state.dask_client,
            ),
        )
    )

    return AnalysisService(
        analysis_repository=get_analysis_repository(),
        analyzer=TreeCoverLossAnalyzer(compute_engine),
        event=ANALYTICS_NAME,
    )


@router.post(
    "/analytics",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    status_code=202,
)
async def create(
    *,
    data: TreeCoverLossAnalyticsIn,
    request: Request,
    background_tasks: BackgroundTasks,
    service: AnalysisService = Depends(create_analysis_service),
):
    return await create_analysis(
        data=data,
        service=service,
        request=request,
        background_tasks=background_tasks,
        resource_link_callback=_datamart_resource_link_response,
    )


def _datamart_resource_link_response(request, service) -> str:
    return str(
        request.url_for(
            "get_tcl_analytics_result", resource_id=service.resource_thumbprint()
        )
    )


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=TreeCoverLossAnalyticsResponse,
    status_code=200,
)
async def get_tcl_analytics_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    analytics_out: AnalyticsOut = await get_analysis(
        resource_id=resource_id,
        analysis_repository=analysis_repository,
        response=response,
    )

    return TreeCoverLossAnalyticsResponse(
        data=TreeCoverLossAnalytics(**analytics_out.model_dump()), status="success"
    )
