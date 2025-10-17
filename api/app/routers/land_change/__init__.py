from fastapi import APIRouter

from .carbon_flux import carbon_flux
from .deforestation_luc_emissions_factor import deforestation_luc_emissions_factor
from .dist_alerts import dist_alerts
from .grasslands import grasslands
from .land_cover import land_cover_change, land_cover_composition
from .natural_lands import natural_lands
from .tree_cover import tree_cover
from .tree_cover_gain import tree_cover_gain
from .tree_cover_loss import tree_cover_loss

router = APIRouter(prefix="/v0/land_change", tags=["β Land Change"])
router.include_router(dist_alerts.router)
router.include_router(grasslands.router)
router.include_router(tree_cover.router)
router.include_router(tree_cover_loss.router)
router.include_router(tree_cover_gain.router)
router.include_router(land_cover_change.router)
router.include_router(land_cover_composition.router)
router.include_router(natural_lands.router)
router.include_router(carbon_flux.router)
router.include_router(deforestation_luc_emissions_factor.router)
