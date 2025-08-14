import logging
import traceback
import uuid
from uuid import UUID, uuid5

from app.domain.analyzers.land_cover_change_analyzer import LandCoverChangeAnalyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover import (
    LandCoverChangeAnalytics,
    LandCoverChangeAnalyticsIn,
)
from app.use_cases.analysis.analysis_service import AnalysisService


class LandCoverChangeService(AnalysisService):
    def __init__(
        self, analysis_repository: AnalysisRepository, analyzer: LandCoverChangeAnalyzer
    ):
        self.analysis_repository = analysis_repository
        self.analyzer = analyzer
        self.analytics_resource: LandCoverChangeAnalytics = (
            LandCoverChangeAnalytics()
        )  # Dummy
        self.analytics_resource_id = uuid5(
            uuid.NAMESPACE_OID, str(uuid.uuid4())
        )  # Dummy

    async def do(self) -> None:
        try:
            if self.analytics_resource.status is not None:
                return  # analysis is in progress, complete, or failed

            self.analytics_resource.status = AnalysisStatus.pending

            analysis = Analysis(
                metadata=self.analytics_resource.metadata,
                result=self.analytics_resource.result,
                status=self.analytics_resource.status,
            )

            await self.analysis_repository.store_analysis(
                self.analytics_resource_id, analysis
            )
            await self.analyzer.analyze(analysis)
        except Exception as e:
            logging.error(
                {
                    "event": "land_cover_change_analytics_processing_failure",
                    "severity": "high",
                    "metadata": self.analytics_resource.metadata,
                    "analysis_repository": self.analysis_repository,
                    "analyzer": self.analyzer,
                    "error_type": e.__class__.__name__,
                    "error_details": str(e),
                    "stack_trace": traceback.format_exc(),
                }
            )

    async def set_resource_from(self, data: LandCoverChangeAnalyticsIn):
        analysis: Analysis = await self.analysis_repository.load_analysis(
            data.thumbprint()
        )
        self.analytics_resource_id = data.thumbprint()
        self.analytics_resource = LandCoverChangeAnalytics(
            metadata=analysis.metadata or data.model_dump(),
            result=analysis.result,
            status=analysis.status,
        )
