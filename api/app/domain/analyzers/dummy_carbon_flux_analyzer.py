from random import random

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.land_change.carbon_flux import CarbonFluxAnalyticsIn


class DummyCarbonFluxAnalyzer(Analyzer):
    async def analyze(self, analysis: Analysis):
        carbon_flux_analytics_in = CarbonFluxAnalyticsIn(**analysis.metadata)
        return {
            "aoi_id": analysis.metadata["aoi"]["ids"],
            "aoi_type": [carbon_flux_analytics_in.aoi.type],
            "carbon_net_flux_Mg_CO2e": [random() * 100],
            "carbon_gross_emissions_Mg_CO2e": [random() * 100],
            "carbon_gross_removals_Mg_CO2e": [random() * 100],
        }
