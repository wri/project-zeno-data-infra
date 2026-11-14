import pytest
from pydantic import ValidationError

from app.models.land_change.deforestation import (
    DeforestationAnalyticsIn,
    EudrSupplyChainDeforestationAnalyticsIn,
)

AOI = {"type": "admin", "ids": ["BRA"]}


def test_deforestation_becomes_tree_cover_loss_in_the_chosen_forest():
    request = DeforestationAnalyticsIn(aoi=AOI, start_year="2002", end_year="2024")

    loss = request.tree_cover_loss_request("intact_forest")

    assert loss.forest_filter == "intact_forest"
    assert loss.canopy_cover == 30
    assert (loss.start_year, loss.end_year) == ("2002", "2024")
    assert loss.intersections == []


def test_deforestation_starts_with_primary_forest_data():
    with pytest.raises(ValidationError):
        DeforestationAnalyticsIn(aoi=AOI, start_year="2001", end_year="2024")


def test_eudr_becomes_tree_cover_loss_in_natural_forest():
    request = EudrSupplyChainDeforestationAnalyticsIn(
        aoi=AOI, start_year="2021", end_year="2024", intersections=["driver"]
    )

    loss = request.tree_cover_loss_request()

    assert loss.forest_filter == "natural_forest_only"
    assert loss.canopy_cover == 10
    assert loss.intersections == ["driver"]


def test_eudr_only_covers_loss_after_2020():
    with pytest.raises(ValidationError):
        EudrSupplyChainDeforestationAnalyticsIn(
            aoi=AOI, start_year="2020", end_year="2024"
        )
