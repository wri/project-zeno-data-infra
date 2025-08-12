import logging
import traceback
import uuid

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus, AnalyticsIn, AnalyticsOut


class AnalysisService:
    def __init__(
        self, analysis_repository: AnalysisRepository, analyzer: Analyzer, event: str
    ):
        self.analysis_repository = analysis_repository
        self.analyzer = analyzer
        self.event = event
        self.analytics_resource: AnalyticsOut = AnalyticsOut()  # Dummy
        self.analytics_resource_id: uuid.UUID = uuid.uuid5(
            uuid.NAMESPACE_OID, self.event
        )  # Dummy

    def event_name(self) -> str:
        return self.event

    async def do(self) -> None:
        try:
            if self.analytics_resource.metadata is None:
                raise Exception("Set analysis resource before calling this method")

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
                    "event": f"{self.event}_analytics_processing_failure",
                    "severity": "high",
                    "metadata": self.analytics_resource.metadata,
                    "analysis_repository": self.analysis_repository,
                    "analyzer": self.analyzer,
                    "error_type": e.__class__.__name__,
                    "error_details": str(e),
                    "stack_trace": traceback.format_exc(),
                }
            )

    def get_status(self) -> AnalysisStatus:
        return self.analytics_resource.status or AnalysisStatus.pending

    async def set_resource_from(self, data: AnalyticsIn):
        analysis: Analysis = await self.analysis_repository.load_analysis(
            data.thumbprint()
        )
        self.analytics_resource_id = data.thumbprint()
        self.analytics_resource = AnalyticsOut(
            metadata=analysis.metadata or data.model_dump(),
            result=analysis.result,
            status=analysis.status,
        )

    def resource_thumbprint(self) -> uuid.UUID:
        return self.analytics_resource_id
