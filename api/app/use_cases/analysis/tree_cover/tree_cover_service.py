import pandas as pd
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


class TreeCoverService:
    def __init__(self, utils=None):
        pass

    async def do(self, tree_cover_analytics: TreeCoverAnalyticsIn):
        pass

    async def compute(self, tree_cover_analytics: TreeCoverAnalyticsIn):
        """
        Placeholder for the actual computation logic.
        This method should implement the logic to compute tree cover analytics.
        """
        analyzer = get_tree_cover_analyzer(tree_cover_analytics)
        return await analyzer.compute()


def get_tree_cover_analyzer(tree_cover_analytics: TreeCoverAnalyticsIn):
    if tree_cover_analytics.aoi.type == "admin":
        return GadmTreeCoverAnalyzer(
            tree_cover_analytics.aoi.type, tree_cover_analytics.aoi.ids
        )


class GadmTreeCoverAnalyzer:
    def __init__(self, tree_cover_analytics_in: TreeCoverAnalyticsIn):
        self.tree_cover_analytics_in = tree_cover_analytics_in

    async def compute(self) -> dict:
        iso_results = self.query_for_results(admin_level)
        adm1_results = self.query_for_results(admin_level)
        adm2_results = self.query_for_results(admin_level)

        combined_results = pd.concat([iso_results, adm1_results, adm2_results])
        return combined_results.dict(orient="list")

    async def query_for_results(admin_level):
        # get query for admin level
        # send request to data api
        # convert to create new results
        pass

    def merge_back_gadm_ids(
        self, admin_level: int, results: pd.DataFrame
    ) -> pd.DataFrame:
        # Join 'iso', 'adm1', and 'adm2' columns into a new 'id' column separated by '.'
        join_cols = ["iso"]
        if admin_level >= 1:
            join_cols.append("adm1")
        elif admin_level >= 2:
            join_cols.append("adm2")

        results["id"] = results[join_cols].astype(str).agg(".".join, axis=1)

        # Drop the original 'iso', 'adm1', and 'adm2' columns if they exist
        results = results.drop(columns=join_cols)
        return results

    def create_query_from_analytics(self) -> tuple[str, str, str]:
        """
        Creates a Query object based on the fields of TreeCoverAnalyticsIn.
        """

        iso_query = self.build_sql_for_admin_level(
            self.tree_cover_analytics_in.aoi.ids,
            0,
            self.tree_cover_analytics_in.canopy_cover,
            self.tree_cover_analytics_in.forest_filter,
        )

        adm1_query = self.build_sql_for_admin_level(
            self.tree_cover_analytics_in.aoi.ids,
            1,
            self.tree_cover_analytics_in.canopy_cover,
            self.tree_cover_analytics_in.forest_filter,
        )

        adm2_query = self.build_sql_for_admin_level(
            self.tree_cover_analytics_in.aoi.ids,
            2,
            self.tree_cover_analytics_in.canopy_cover,
            self.tree_cover_analytics_in.forest_filter,
        )

        return (iso_query, adm1_query, adm2_query)

    def build_sql_for_admin_level(
        self, ids, admin_level, canopy_cover, forest_filter=None
    ):
        admin_level_ids = []
        for id_str in ids:
            parts = id_str.split(".")
            if len(parts) == 1 and admin_level == 0:
                admin_level_ids.append(parts[0])
            elif len(parts) == 2 and admin_level == 1:
                admin_level_ids.append((parts[0], int(parts[1])))
            elif len(parts) == 3 and admin_level == 2:
                admin_level_ids.append((parts[0], int(parts[1]), int(parts[2])))

        if admin_level == 0:
            admin_select_fields = "iso"
            admin_filter_fields = "iso"
        elif admin_level == 1:
            admin_select_fields = "iso, adm1"
            admin_filter_fields = "(iso, adm1)"
        elif admin_level == 2:
            admin_select_fields = "iso, adm1, adm2"
            admin_filter_fields = "(iso, adm1, adm2)"

        select_str = f"SELECT {admin_select_fields}, SUM(umd_tree_cover_extent_2000__ha) AS umd_tree_cover_extent_2000__ha FROM data"
        where_str = f"WHERE umd_tree_cover_density_2000__threshold = {canopy_cover} AND {admin_filter_fields} in {tuple(admin_level_ids)}"
        if forest_filter is not None:
            if forest_filter == "primary_forest":
                forest_filter_field = "is__umd_regional_primary_forest_2001"
            elif forest_filter == "intact_forest":
                forest_filter_field = "is__ifl_intact_forest_landscapes_2000"
            where_str += f" AND {forest_filter_field} = true"
        group_by = f"GROUP BY {admin_select_fields}"
        query = f"{select_str} {where_str} {group_by}"
        return query
