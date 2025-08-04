from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


class TreeCoverService:
    def __init__(self, utils=None):
        pass

    def do(self, tree_cover_analytics: TreeCoverAnalyticsIn):
        pass

    def compute(self, tree_cover_analytics: TreeCoverAnalyticsIn):
        """
        Placeholder for the actual computation logic.
        This method should implement the logic to compute tree cover analytics.
        """
        pass

    def create_query_from_analytics(self, tree_cover_analytics: TreeCoverAnalyticsIn):
        """
        Creates a Query object based on the fields of TreeCoverAnalyticsIn.
        """

        if tree_cover_analytics.aoi.type == "admin":
            # Separate ids by their number of parts
            iso_ids = []
            iso_adm1_ids = []
            iso_adm1_adm2_ids = []

            for id_str in tree_cover_analytics.aoi.ids:
                parts = id_str.split(".")
                if len(parts) == 1:
                    iso_ids.append(parts[0])
                elif len(parts) == 2:
                    iso_adm1_ids.append((parts[0], int(parts[1])))
                elif len(parts) == 3:
                    iso_adm1_adm2_ids.append((parts[0], int(parts[1]), int(parts[2])))

            iso_query = self.build_query_for_admin_level(
                iso_ids,
                0,
                tree_cover_analytics.canopy_cover,
                tree_cover_analytics.forest_filter,
            )

            adm1_query = self.build_query_for_admin_level(
                iso_adm1_ids,
                1,
                tree_cover_analytics.canopy_cover,
                tree_cover_analytics.forest_filter,
            )

            adm2_query = self.build_query_for_admin_level(
                iso_adm1_adm2_ids,
                2,
                tree_cover_analytics.canopy_cover,
                tree_cover_analytics.forest_filter,
            )

            return (iso_query, adm1_query, adm2_query)

    def build_query_for_admin_level(
        self, ids, admin_level, canopy_cover, forest_filter=None
    ):
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
        where_str = f"WHERE umd_tree_cover_density_2000__threshold = {canopy_cover} AND {admin_filter_fields} in {tuple(ids)}"
        if forest_filter is not None:
            if forest_filter == "primary_forest":
                forest_filter_field = "is__umd_regional_primary_forest_2001"
            elif forest_filter == "intact_forest":
                forest_filter_field = "is__ifl_intact_forest_landscapes_2000"
            where_str += f" AND {forest_filter_field} = true"
        group_by = f"GROUP BY {admin_select_fields}"
        query = f"{select_str} {where_str} {group_by}"
        return query
