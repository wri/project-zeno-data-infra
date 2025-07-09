from fastapi import APIRouter
from .dist_alerts import dist_alerts

router = APIRouter(prefix="/v0/land_change", tags=["β Land Change"])
router.include_router(dist_alerts.router)
