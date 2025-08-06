from typing import Literal

from app.models.common.areas_of_interest import AreaOfInterest
from app.models.common.base import StrictBaseModel


class ForestLossEvent(StrictBaseModel):
    year: int
    area_ha: float
    carbon_emissions_Mg: float


class ForestLossEvents(StrictBaseModel):
    events: list[ForestLossEvent]
    driver: str


class ForestGainEvent(StrictBaseModel):
    area_ha: float


class ForestGainEvents(StrictBaseModel):
    events: list[ForestGainEvent]


class ForestCriteria(StrictBaseModel):
    canopy_cover_percent: int
    is_primary: bool
    is_intact: bool
    model: Literal["UMD GLAD", "WRI"]


class Forest(StrictBaseModel):
    aoi: AreaOfInterest
    criteria: ForestCriteria
    loss: ForestLossEvents
    gain: ForestGainEvents
    carbon_removals_MgCOe: float
    cabon_net_flux_MgCOe: float
    area_ha: float


# Then the service might look something like:
#
# class GeotrellisTableService():
#     def get_forest(self, aoi: AreaOfInterest, forest: ForestCriteria) -> Forest:
#         # generate base sql for getting forest from geotrellis
#         query = ""
#         if forest.aoi.type == "admin":
#             # figure out how to make where filter based on admin IDs, what queries to make
#             pass
#         elif forest.aoi.type == "protected_area":
#             # figure out where to make admin IDs
#             pass

#         result = send_query(query)
#         forest = Forest(
#             result["umd_tree_cover_loss"],
#             ...
#         )

#         return forest
