from dataclasses import dataclass

from app.models.common.areas_of_interest import (
    DEEPEST_PRECOMPUTED_ADMIN_LEVEL,
    AreaOfInterest,
    admin_level,
)


@dataclass(frozen=True)
class PrecomputedAdminLevels:
    """The admin levels that have precomputed results, from country down to
    `deepest_level`. Other areas are computed on the fly."""

    deepest_level: int = DEEPEST_PRECOMPUTED_ADMIN_LEVEL

    def includes(self, admin_id: str) -> bool:
        return admin_level(admin_id) <= self.deepest_level

    def covers(self, aoi: AreaOfInterest) -> bool:
        """Whether every area of the request has precomputed results."""
        if getattr(aoi, "type", None) != "admin":
            return False
        return all(self.includes(admin_id) for admin_id in aoi.ids)
