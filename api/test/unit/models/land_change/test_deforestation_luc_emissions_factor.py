import pytest

from app.models.land_change.deforestation_luc_emissions_factor import (
    DeforestationLUCEmissionsFactorAnalyticsIn,
)


def test_admin_areas_below_level_2_are_rejected():
    with pytest.raises(ValueError, match="admin level allowed is 2"):
        DeforestationLUCEmissionsFactorAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.12.1.1"]},
            gas_types=["CO2e"],
            crop_types=["Banana"],
            start_year="2021",
            end_year="2023",
        )
