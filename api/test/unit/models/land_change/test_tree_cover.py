import uuid

import pytest

from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


@pytest.fixture()
def base_config():
    """Base configuration for model instances."""
    return TreeCoverAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
        tcd_threshold="30",
    )


class TestTreeCoverAnalyticsIn:
    def test_thumbprint_is_same_for_same_fields(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        assert model.thumbprint() == uuid.UUID("96c865fb-6502-59c2-a6b5-baa2b9be731a")

    def test_thumbprint_changes_when_aoi_changes(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        model.aoi = AdminAreaOfInterest(
            type="admin",
            ids=["BRA.12"],
        )

        assert model.thumbprint() != base_config.thumbprint()

    def test_thumbprint_changes_when_tcd_changes(self, base_config):
        model = TreeCoverAnalyticsIn(**base_config.model_dump())
        model.tcd_threshold = "15"

        assert model.thumbprint() != base_config.thumbprint()



class TestTreeCoverAnalyticsInValidations:
    def test_tcd_must_be_valid(self):
        with pytest.raises(ValueError):
            TreeCoverAnalyticsIn(
                aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
                tcd_threshold="5",
            )

    def test_valid_tcds_work(self):
        TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["BRA.12.1"]),
            tcd_threshold="15",
        )
