from fastapi import APIRouter

from .dist_alerts import dist_alerts
from .land_cover import land_cover_change
from .land_cover import land_cover_composition
from .grasslands import grasslands
from .tree_cover_loss import tree_cover_loss
from .natural_lands import natural_lands

router = APIRouter(prefix="/v0/land_change", tags=["β Land Change"])
router.include_router(dist_alerts.router)
router.include_router(grasslands.router)
router.include_router(tree_cover_loss.router)
router.include_router(land_cover_change.router)
router.include_router(land_cover_composition.router)
router.include_router(natural_lands.router)
