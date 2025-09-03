from app.domain.analyzers.dummy_tree_cover_gain_analyzer import (
    DummyTreeCoverGainAnalyzer,
)
from app.domain.compute_engines.compute_engine import (
    ComputeEngine,
    FloxOTFHandler,
    TreeCoverGainPrecalcHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (
    PrecalcSqlQueryBuilder,
)
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.models.common.analysis import AnalyticsOut
from app.models.common.base import DataMartResourceLinkResponse
from app.models.land_change.tree_cover_gain import (
    TreeCoverGainAnalytics,
    TreeCoverGainAnalyticsIn,
    TreeCoverGainAnalyticsResponse,
)
from app.routers.common_analytics import create_analysis, get_analysis
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from pydantic import UUID5

ANALYTICS_NAME = "tree_cover_gain"
router = APIRouter(prefix=f"/{ANALYTICS_NAME}")


def get_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service(request: Request) -> AnalysisService:
    compute_engine = ComputeEngine(
        handler=TreeCoverGainPrecalcHandler(
            precalc_query_builder=PrecalcSqlQueryBuilder(),
            precalc_query_service=DuckDbPrecalcQueryService(
                table_uri="s3://lcl-analytics/zonal-statistics/admin-tree-cover-gain.parquet"
            ),
            next_handler=FloxOTFHandler(
                dataset_repository=ZarrDatasetRepository(),
                aoi_geometry_repository=DataApiAoiGeometryRepository(),
                dask_client=request.app.state.dask_client,
            ),
        )
    )

    return AnalysisService(
        analysis_repository=get_analysis_repository(),
        analyzer=DummyTreeCoverGainAnalyzer(compute_engine),  # DUMMY DATA
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
    data: TreeCoverGainAnalyticsIn,
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
            "get_tree_cover_gain_analytics_result",
            resource_id=service.resource_thumbprint(),
        )
    )


@router.get(
    "/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=TreeCoverGainAnalyticsResponse,
    status_code=200,
)
async def get_tree_cover_gain_analytics_result(
    resource_id: UUID5,
    response: FastAPIResponse,
    analysis_repository: AnalysisRepository = Depends(get_analysis_repository),
):
    analytics_out: AnalyticsOut = await get_analysis(
        resource_id=resource_id,
        analysis_repository=analysis_repository,
        response=response,
    )

    return TreeCoverGainAnalyticsResponse(
        data=TreeCoverGainAnalytics(**analytics_out.model_dump()), status="success"
    )
