from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)
from app.models.common.areas_of_interest import AreaOfInterest


class AreaOfInterestList:
    def __init__(self, aois: AreaOfInterest, compute_engine: ComputeEngine):
        self.type = aois.type
        self.ids = aois.ids
        self.compute_engine = compute_engine

    async def get_tree_cover_loss(
        self, canopy_cover: int, start_year: int, end_year: int, forest_type: str
    ):
        query = DatasetQuery(
            aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
            group_bys=[Dataset.tree_cover_loss],
            filters=[
                DatasetFilter(
                    dataset=Dataset.canopy_cover, op=">=", value=canopy_cover
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op=">=",
                    value=start_year,
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op="<=",
                    value=end_year,
                ),
            ],
        )
        return await self.compute_engine.compute(self.type, self.ids, query)
