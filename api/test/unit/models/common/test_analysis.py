from app.domain.models.dataset import Dataset
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


def _make_analytics_in(**kwargs) -> TreeCoverLossAnalyticsIn:
    defaults = dict(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.1"]),
        start_year="2020",
        end_year="2021",
        canopy_cover=30,
        intersections=[],
    )
    return TreeCoverLossAnalyticsIn(**{**defaults, **kwargs})


class TestThumbprint:
    def test_thumbprint_without_set_environment_defaults_to_production(self):
        a = _make_analytics_in()
        b = _make_analytics_in()
        b.set_environment(Environment.production)
        assert a.thumbprint() == b.thumbprint()

    def test_identical_requests_same_environment_same_thumbprint(self):
        a = _make_analytics_in()
        b = _make_analytics_in()
        a.set_environment(Environment.production)
        b.set_environment(Environment.production)
        assert a.thumbprint() == b.thumbprint()

    def test_different_environments_with_different_uris_different_thumbprints(self):
        staging_uri = "s3://lcl-analytics/zarr/umd_tree_cover_loss/v1.99/year.zarr"
        original = ZarrDatasetRepository._ZARR_URIS[Environment.staging].copy()
        try:
            ZarrDatasetRepository._ZARR_URIS[Environment.staging][
                Dataset.tree_cover_loss
            ] = staging_uri

            a = _make_analytics_in()
            b = _make_analytics_in()
            a.set_environment(Environment.production)
            b.set_environment(Environment.staging)
            assert a.thumbprint() != b.thumbprint()
        finally:
            ZarrDatasetRepository._ZARR_URIS[Environment.staging] = original

    def test_promoting_staging_uri_to_production_makes_thumbprints_equal(self):
        """When staging is promoted (URIs become identical), cached results
        should be shared — thumbprints must match."""
        prod_uri = ZarrDatasetRepository.resolve_zarr_uri(
            Dataset.tree_cover_loss, Environment.production
        )
        original = ZarrDatasetRepository._ZARR_URIS[Environment.staging].copy()
        try:
            # Simulate promotion: staging now points to the same URI as production
            ZarrDatasetRepository._ZARR_URIS[Environment.staging][
                Dataset.tree_cover_loss
            ] = prod_uri

            a = _make_analytics_in()
            b = _make_analytics_in()
            a.set_environment(Environment.production)
            b.set_environment(Environment.staging)
            assert a.thumbprint() == b.thumbprint()
        finally:
            ZarrDatasetRepository._ZARR_URIS[Environment.staging] = original

    def test_different_request_params_different_thumbprints(self):
        a = _make_analytics_in(start_year="2015")
        b = _make_analytics_in(start_year="2020")
        a.set_environment(Environment.production)
        b.set_environment(Environment.production)
        assert a.thumbprint() != b.thumbprint()
