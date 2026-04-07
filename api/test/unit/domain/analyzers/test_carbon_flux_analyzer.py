import uuid

from app.domain.analyzers.carbon_flux_analyzer import INPUT_URIS, CarbonFluxAnalyzer
from app.domain.models.environment import Environment


class TestCarbonFluxAnalyzerThumbprint:
    def test_thumbprint_returns_a_uuid(self):
        analyzer = CarbonFluxAnalyzer(input_uris=INPUT_URIS[Environment.production])
        assert isinstance(analyzer.thumbprint(), uuid.UUID)

    def test_thumbprint_stable_with_same_input_uris(self):
        a = CarbonFluxAnalyzer(input_uris=INPUT_URIS[Environment.production])
        b = CarbonFluxAnalyzer(input_uris=INPUT_URIS[Environment.production])
        assert a.thumbprint() == b.thumbprint()

    def test_thumbprint_changes_when_input_uris_change(self):
        """Simulates deploying new data: the resource ID must change so stale
        cached results are not served."""
        production = CarbonFluxAnalyzer(input_uris=INPUT_URIS[Environment.production])
        different = CarbonFluxAnalyzer(
            input_uris={
                **INPUT_URIS[Environment.production],
                "zarr_uri": "s3://new/path.zarr",
            }
        )
        assert production.thumbprint() != different.thumbprint()

    def test_production_and_staging_thumbprints_differ_when_uris_differ(self):
        production = CarbonFluxAnalyzer(input_uris=INPUT_URIS[Environment.production])
        staging = CarbonFluxAnalyzer(input_uris=INPUT_URIS[Environment.staging])
        # Only meaningful if staging has distinct URIs; if identical, they should match
        if INPUT_URIS[Environment.production] != INPUT_URIS[Environment.staging]:
            assert production.thumbprint() != staging.thumbprint()
        else:
            assert production.thumbprint() == staging.thumbprint()
