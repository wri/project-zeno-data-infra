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

            def build_query_for_admin_level(
                ids, admin_level, canopy_cover, forest_filter=None
            ):
                if admin_level == 0:
                    select_str = "SELECT iso, SUM(umd_tree_cover_extent_2000__ha) AS umd_tree_cover_extent_2000__ha FROM data"
                    where_str = f"WHERE umd_tree_cover_density_2000__threshold = {canopy_cover} AND iso in {tuple(ids)}"
                    if forest_filter is not None:
                        if forest_filter == "primary_forest":
                            forest_filter_field = "is__umd_regional_primary_forest_2001"
                        elif forest_filter == "intact_forest":
                            forest_filter_field = (
                                "is__ifl_intact_forest_landscapes_2000"
                            )
                        where_str += f" AND {forest_filter_field} = true"
                    group_by = "GROUP BY iso"
                    return f"{select_str} {where_str} {group_by}"

            iso_query = build_query_for_admin_level(
                iso_ids,
                0,
                tree_cover_analytics.canopy_cover,
                tree_cover_analytics.forest_filter,
            )
            return iso_query

            # if iso_ids:
            #     iso_list = ", ".join([f"'{iso}'" for iso in iso_ids])
            #     where_clauses.append(f"iso IN ({iso_list})")

            # if iso_adm1_ids:
            #     adm1_list = ", ".join([f"('{iso}', {adm1})" for iso, adm1 in iso_adm1_ids])
            #     where_clauses.append(f"(iso, adm1) IN ({adm1_list})")

            # if iso_adm1_adm2_ids:
            #     adm2_list = ", ".join([f"('{iso}', {adm1}, {adm2})" for iso, adm1, adm2 in iso_adm1_adm2_ids])
            #     where_clauses.append(f"(iso, adm1, adm2) IN ({adm2_list})")

            # # Combine all where clauses with OR (since they are for different levels)
            # if where_clauses:
            #     where_clause = "WHERE " + " OR ".join(where_clauses)
            # else:
            #     where_clause = ""
