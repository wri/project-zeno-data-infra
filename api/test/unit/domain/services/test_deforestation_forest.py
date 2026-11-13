from shapely.geometry import box

from app.domain.services.deforestation_forest import DeforestationForest

forest = DeforestationForest(tropics_latitude=30)
brazil_like = box(-74, -34, -34, 5)
russia_like = box(27, 41, 180, 82)


def test_area_centred_in_the_tropics_uses_primary_forest():
    assert forest.forest_filter([brazil_like]) == "primary_forest"


def test_area_centred_outside_the_tropics_uses_intact_forest():
    assert forest.forest_filter([russia_like]) == "intact_forest"


def test_any_area_centred_outside_the_tropics_uses_intact_forest():
    assert forest.forest_filter([brazil_like, russia_like]) == "intact_forest"


def test_no_geometry_uses_intact_forest():
    assert forest.forest_filter([]) == "intact_forest"
