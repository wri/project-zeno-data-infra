import newrelic.agent as nr_agent

from app.analysis.common.analysis import get_sql_in_list
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.land_change.deforestation_luc_emissions_factor import (
    DeforestationLUCEmissionsFactorAnalyticsIn,
)


class DeforestationLUCEmissionsFactorAnalyzer(Analyzer):
    """Get the emissions factor, emissions and crop production yields based on land use change."""

    def __init__(
        self,
        compute_engine=None,
        dataset_repository=None,
        query_service=None,
        table_uri: str | None = None,
    ):
        self.compute_engine = compute_engine
        self.dataset_repository = dataset_repository
        self.query_service = query_service
        self._table_uri = table_uri

    @nr_agent.function_trace(name="DeforestationLUCEmissionsFactorAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        deforestation_luc_emissions_factor_analytics_in = (
            DeforestationLUCEmissionsFactorAnalyticsIn(**analysis.metadata)
        )

        if deforestation_luc_emissions_factor_analytics_in.aoi.type == "admin":
            results = await self.analyze_admin_areas(
                deforestation_luc_emissions_factor_analytics_in
            )

        else:
            raise NotImplementedError()

        analysis.result = results

    async def analyze_admin_areas(self, analytics_in):
        aoi_ids = get_sql_in_list(analytics_in.aoi.ids)
        gas_types = get_sql_in_list(analytics_in.gas_types)
        crop_types = get_sql_in_list(analytics_in.crop_types)
        query = f"select * from data_source where aoi_id in {aoi_ids} and gas_type in {gas_types} and crop_type in {crop_types} and year >= {analytics_in.start_year} and year <= {analytics_in.end_year}"

        df = await self.query_service.execute(query)
        df["aoi_type"] = ["admin"] * len(df["aoi_id"])

        return df

    def input_uris(self) -> list[str]:
        uris = []
        if self._table_uri is not None:
            uris.append(self._table_uri)
        return sorted(uris)
