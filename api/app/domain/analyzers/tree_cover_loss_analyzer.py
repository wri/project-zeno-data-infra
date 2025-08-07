import pandas as pd
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class TreeCoverLossAnalyzer(Analyzer):
    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository  # TreeCoverLossRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.

    async def analyze(self, analysis: Analysis):
        tree_cover_loss_analytics_in = TreeCoverLossAnalyticsIn(**analysis.metadata)
        if tree_cover_loss_analytics_in.aoi.type == "admin":
            iso_results = await self._get_results_for_admin_level(
                0, tree_cover_loss_analytics_in
            )
            adm1_results = await self._get_results_for_admin_level(
                1, tree_cover_loss_analytics_in
            )
            adm2_results = await self._get_results_for_admin_level(
                1, tree_cover_loss_analytics_in
            )

            iso_results_id_merged = self._merge_back_gadm_ids(0, iso_results)
            adm1_results_id_merged = self._merge_back_gadm_ids(1, adm1_results)
            adm2_results_id_merged = self._merge_back_gadm_ids(2, adm2_results)

            combined_results = pd.concat(
                [iso_results_id_merged, adm1_results_id_merged, adm2_results_id_merged]
            )
            analyzed_analysis = Analysis(
                combined_results, analysis.metadata, AnalysisStatus.saved
            )

            self.analysis_repository.store_analysis(
                tree_cover_loss_analytics_in.thumbprint(), analyzed_analysis
            )
        else:
            raise NotImplementedError()

    async def _get_results_for_admin_level(
        self, admin_level, tree_cover_loss_analytics_in: TreeCoverLossAnalyticsIn
    ) -> pd.DataFrame:
        admin_level_ids = tree_cover_loss_analytics_in.aoi.get_ids_at_admin_level(
            admin_level
        )
        if not admin_level_ids:
            cols = ["iso"]
            if admin_level >= 1:
                cols.append("adm1")
            if admin_level >= 2:
                cols.append("adm2")

            cols += [
                "umd_tree_cover_loss__ha",
                "gfw_gross_emissions_co2e_all_gases__Mg",
                "umd_tree_cover_loss__year",
            ]
            return pd.DataFrame(columns=cols)

        if admin_level == 0:
            admin_select_fields = "iso"
            admin_filter_fields = "iso"
        elif admin_level == 1:
            admin_select_fields = "iso, adm1"
            admin_filter_fields = "(iso, adm1)"
        elif admin_level == 2:
            admin_select_fields = "iso, adm1, adm2"
            admin_filter_fields = "(iso, adm1, adm2)"

        select_str = f'SELECT {admin_select_fields}, umd_tree_cover_loss__year, SUM(umd_tree_cover_loss__ha) AS umd_tree_cover_loss__ha, SUM("gfw_gross_emissions_co2e_all_gases__Mg") AS "gfw_gross_emissions_co2e_all_gases__Mg") FROM data'
        where_str = f"WHERE umd_tree_cover_density_2000__threshold = {tree_cover_loss_analytics_in.canopy_cover} AND {admin_filter_fields} in {tuple(admin_level_ids)}"
        if tree_cover_loss_analytics_in.forest_filter is not None:
            if tree_cover_loss_analytics_in.forest_filter == "primary_forest":
                forest_filter_field = "is__umd_regional_primary_forest_2001"
            elif tree_cover_loss_analytics_in.forest_filter == "intact_forest":
                forest_filter_field = "is__ifl_intact_forest_landscapes_2000"
            where_str += f" AND {forest_filter_field} = true"

        group_by = f"GROUP BY {admin_select_fields}, umd_tree_cover_loss__year"
        query = f"{select_str} {where_str} {group_by}"

        if admin_level == 0:
            dataset = "gadm__tcl__iso_change"
        elif admin_level == 1:
            dataset = "gadm__tcl__adm1_change"
        else:
            dataset = "gadm__tcl__adm2_change"

        version = "v20250515"

        results = await self.compute_engine.compute(
            {"dataset": dataset, "version": version, "query": query}
        )
        df = pd.DataFrame(results)
        return df

    def _merge_back_gadm_ids(
        self, admin_level: int, results: pd.DataFrame
    ) -> pd.DataFrame:
        # Join 'iso', 'adm1', and 'adm2' columns into a new 'id' column separated by '.'
        join_cols = ["iso"]
        if admin_level >= 1:
            join_cols.append("adm1")
        elif admin_level >= 2:
            join_cols.append("adm2")

        if results.empty:
            results["id"] = None
        else:
            results["id"] = results[join_cols].astype(str).agg(".".join, axis=1)

        # Drop the original 'iso', 'adm1', and 'adm2' columns if they exist
        results = results.drop(columns=join_cols)
        return results
