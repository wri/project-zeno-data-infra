from fastapi import FastAPI

from app.domain.analyzers.natural_lands_analyzer import NATURAL_LANDS_CLASSES
from app.routers.land_change.natural_lands.natural_lands import router


def test_the_dataset_card_names_every_class():
    app = FastAPI()
    app.include_router(router)

    operation = app.openapi()["paths"]["/natural_lands/analytics"]["post"]
    measures = operation["x-dataset"]["measures"]

    for name in NATURAL_LANDS_CLASSES.values():
        assert name in measures
