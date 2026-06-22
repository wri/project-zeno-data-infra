from typing import Dict

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis


class ZonalStatisticsAnalyzer(Analyzer):
    """Routes an analysis to a precomputed source (admin AOIs) or an on-the-fly
    computation (all other AOI types).

    The dataset-specific behaviour lives entirely in the injected collaborators,
    so new datasets are added by composing this class rather than modifying it.
    """

    def __init__(
        self,
        model,
        admin_source,
        on_the_fly,
        input_uris: Dict[str, str] | None = None,
    ):
        self.model = model
        self.admin_source = admin_source
        self.on_the_fly = on_the_fly
        self.input_uris = input_uris  # retained for thumbprint()/resource_id

    async def analyze(self, analysis: Analysis) -> None:
        analytics_in = self.model(**analysis.metadata)

        if analytics_in.aoi.type == "admin":
            analysis.result = await self.admin_source.get(analytics_in)
        else:
            analysis.result = await self.on_the_fly.compute(analytics_in)
