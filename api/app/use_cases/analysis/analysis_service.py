import logging
import traceback
import uuid

import newrelic.agent as nr_agent

from app.analysis.common.analysis import FeatureTooSmallError
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

    @nr_agent.background_task(name="AnalysisService.do", group="Task")
    async def do(self) -> None:
        try:
            if self.analytics_resource.metadata is None:
                raise Exception("Set analysis resource before calling this method")

            if self.analytics_resource.status is not None:
                return  # analysis is in progress, complete, or failed

            metadata = self.analytics_resource.metadata.copy()
            aoi = metadata.pop("aoi")

            nr_agent.add_custom_attributes(
                {
                    "args.metadata": {
                        **metadata,
                        "aoi_type": aoi.get("type"),
                        **(
                            {
                                "aoi_count": len(
                                    aoi.get("feature_collection", {}).get(
                                        "features", []
                                    )
                                )
                            }
                            if aoi.get("type") == "feature_collection"
                            else {
                                "aoi_ids_sample": aoi.get("ids", [])[:10],
                                "aoi_count": len(aoi.get("ids", [])),
                            }
                        ),
                    }
                }.items()
            )
            self.analytics_resource.status = AnalysisStatus.pending
            analysis = Analysis(
                metadata=self.analytics_resource.metadata,
                result=self.analytics_resource.result,
                status=self.analytics_resource.status,
            )
            await self.analysis_repository.store_analysis(
                self.resource_thumbprint(), analysis
            )

            await self.analyzer.analyze(analysis)

            self.analytics_resource.status = AnalysisStatus.saved
            self.analytics_resource.result = analysis.result
            await self.analysis_repository.store_analysis(
                self.resource_thumbprint(),
                Analysis(
                    metadata=self.analytics_resource.metadata,
                    result=self.analytics_resource.result,
                    status=self.analytics_resource.status,
                ),
            )
        except FeatureTooSmallError as e:
            logging.warning(
                {
                    "event": f"{self.event}_analytics_feature_too_small",
                    "severity": "medium",
                    "metadata": self.analytics_resource.metadata,
                    "error_details": str(e),
                }
            )
            self.analytics_resource.status = AnalysisStatus.failed
            self.analytics_resource.result = {"error": str(e)}
            await self.analysis_repository.store_analysis(
                self.resource_thumbprint(),
                Analysis(
                    metadata=self.analytics_resource.metadata,
                    result=self.analytics_resource.result,
                    status=self.analytics_resource.status,
                ),
            )
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
            self.analytics_resource.status = AnalysisStatus.failed
            await self.analysis_repository.store_analysis(
                self.resource_thumbprint(),
                Analysis(
                    metadata=self.analytics_resource.metadata,
                    result=self.analytics_resource.result,
                    status=self.analytics_resource.status,
                ),
            )

    def get_status(self) -> AnalysisStatus:
        return self.analytics_resource.status or AnalysisStatus.pending

    async def set_resource_from(self, data: AnalyticsIn):
        self.analytics_resource_id = data.thumbprint()

        analysis: Analysis = await self.analysis_repository.load_analysis(
            self.resource_thumbprint()
        )

        self.analytics_resource = AnalyticsOut(
            metadata=analysis.metadata or data.model_dump(),
            result=analysis.result,
            status=analysis.status,
        )

        if self.analytics_resource.status is None:  # store placeholder immediately
            await self.analysis_repository.store_analysis(
                self.resource_thumbprint(),
                Analysis(
                    metadata=self.analytics_resource.metadata, result=None, status=None
                ),
            )

    def resource_thumbprint(self) -> uuid.UUID:
        """Combine the request fingerprint with the analyzer's dataset fingerprint.

        This means the cache key changes when either the request parameters
        change (via data.thumbprint()) or the input datasets change (via
        analyzer.thumbprint()).  Neither fingerprint needs to be stored
        separately — recomputing on each request is cheap and correct.
        """
        return uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{self.analytics_resource_id}{self.analyzer.thumbprint()}",
        )
