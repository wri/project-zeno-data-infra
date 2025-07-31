from app.models.common.query import Filter, Query
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
        aoi_filter = Filter(
            field=tree_cover_analytics.aoi.type,
            op="in",
            value=tree_cover_analytics.aoi.ids,
        )

        forest_filter = Filter(
            field=tree_cover_analytics.forest_filter,
            op="=",
            value=True,
        )

        canopy_cover_filter = Filter(
            field="canopy_cover",
            op=">",
            value=tree_cover_analytics.canopy_cover,
        )

        dataset = "tree_cover"

        return Query(
            dataset=dataset, filters=[aoi_filter, forest_filter, canopy_cover_filter]
        )
