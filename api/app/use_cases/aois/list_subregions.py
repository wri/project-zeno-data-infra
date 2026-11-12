from dataclasses import dataclass
from typing import Dict, List, Optional

from shapely.ops import unary_union

from app.domain.models.aoi_subregion import AoiSubregion
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.repositories.aoi_subregions import (
    AdminSubregions,
    IntersectingSubregions,
)
from app.models.common.areas_of_interest import admin_level

# The admin id that stands for the whole world, whose subregions are countries.
WORLD_ID = "WLD"


class UnsupportedSubregionsError(ValueError):
    pass


@dataclass(frozen=True)
class Subregions:
    """The largest subregions of an AOI, and how many it has in all."""

    total: int
    subregions: List[AoiSubregion]


class ListSubregions:
    """Lists the AOIs of a type that lie within an AOI, largest first. Admin
    areas below an admin area are found by their ids; other types by their
    geometry."""

    def __init__(
        self,
        admin: AdminSubregions,
        intersecting: Dict[str, IntersectingSubregions],
        geometries: AoiGeometryRepository,
    ):
        self.admin = admin
        self.intersecting = intersecting
        self.geometries = geometries

    async def within(
        self, parent, area_type: str, level: Optional[int], limit: int
    ) -> Subregions:
        if area_type == "admin":
            found = await self.admin_within(parent, level)
        else:
            found = await self.intersecting_within(parent, area_type)
        largest_first = sorted(found, key=lambda area: area.area_ha, reverse=True)
        return Subregions(total=len(found), subregions=largest_first[:limit])

    async def admin_within(self, parent, level: Optional[int]) -> List[AoiSubregion]:
        if parent.type != "admin":
            raise UnsupportedSubregionsError(
                "Admin subregions can only be listed for admin areas"
            )
        parent_id = parent.ids[0]
        if parent_id == WORLD_ID:
            return await self.admin.within(None, level or 0)
        parent_level = admin_level(parent_id)
        level = parent_level + 1 if level is None else level
        if level <= parent_level:
            raise UnsupportedSubregionsError(
                f"Level {level} is not below {parent_id}, at level {parent_level}"
            )
        return await self.admin.within(parent_id, level)

    async def intersecting_within(self, parent, area_type: str) -> List[AoiSubregion]:
        if area_type not in self.intersecting or parent.ids == [WORLD_ID]:
            raise UnsupportedSubregionsError(
                f"Subregions of type {area_type} can't be listed for this area"
            )
        geometries, _ = await self.geometries.load(parent)
        return await self.intersecting[area_type].within(unary_union(geometries))
