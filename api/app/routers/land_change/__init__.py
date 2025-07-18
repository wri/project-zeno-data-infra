from fastapi import APIRouter
from .dist_alerts import dist_alerts
from .tree_cover_loss import tree_cover_loss

router = APIRouter(prefix="/v0/land_change", tags=["β Land Change"])
router.include_router(dist_alerts.router)
router.include_router(tree_cover_loss.router)
