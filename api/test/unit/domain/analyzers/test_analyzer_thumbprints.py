import uuid
from importlib import import_module

import pytest

from app.domain.models.environment import Environment

ANALYZER_MODULES = [
    ("carbon_flux_analyzer", "CarbonFluxAnalyzer"),
    (
        "deforestation_luc_emissions_factor_analyzer",
        "DeforestationLUCEmissionsFactorAnalyzer",
    ),
    ("dist_alerts_analyzer", "DistAlertsAnalyzer"),
    ("grasslands_analyzer", "GrasslandsAnalyzer"),
    ("land_cover_change_analyzer", "LandCoverChangeAnalyzer"),
    ("land_cover_composition_analyzer", "LandCoverCompositionAnalyzer"),
    ("natural_lands_analyzer", "NaturalLandsAnalyzer"),
    ("tree_cover_analyzer", "TreeCoverAnalyzer"),
    ("tree_cover_gain_analyzer", "TreeCoverGainAnalyzer"),
    ("tree_cover_loss_analyzer", "TreeCoverLossAnalyzer"),
]


class TestAnalyzerThumbprints:

    @pytest.mark.parametrize("module_name, class_name", ANALYZER_MODULES)
    def test_thumbprint_stable_with_same_input_uris(
        self, module_name: str, class_name: str
    ):
        # Is this too clever? This is just barely possible because each module
        # has a variable called INPUT_URIS. It was made so for consistency but
        # I didn't mean to rely on it like this.
        module_under_test = import_module(f"app.domain.analyzers.{module_name}")
        input_uris = getattr(module_under_test, "INPUT_URIS")
        class_under_test = getattr(module_under_test, class_name)

        # Is this too clever? This is just barely possible because the
        # compute_engine kwarg is ignored by those Analyzers that don't need
        # it.
        analyzer_a = class_under_test(
            compute_engine=None, input_uris=input_uris[Environment.production]
        )
        analyzer_b = class_under_test(
            compute_engine=None, input_uris=input_uris[Environment.production]
        )

        assert analyzer_a.thumbprint() == analyzer_b.thumbprint()
        assert isinstance(analyzer_a.thumbprint(), uuid.UUID)

    @pytest.mark.parametrize("module_name, class_name", ANALYZER_MODULES)
    def test_thumbprint_changes_when_input_uris_change(
        self, module_name: str, class_name: str
    ):
        """Simulates deploying new data: the resource ID must change so stale
        cached results are not served."""
        module_under_test = import_module(f"app.domain.analyzers.{module_name}")
        input_uris = getattr(module_under_test, "INPUT_URIS")
        class_under_test = getattr(module_under_test, class_name)

        production = class_under_test(
            compute_engine=None, input_uris=input_uris[Environment.production]
        )
        different = class_under_test(
            compute_engine=None,
            input_uris={
                **input_uris[Environment.production],
                "some_zarr_uri": "s3://new/path.zarr",
            },
        )

        assert production.thumbprint() != different.thumbprint()

    @pytest.mark.parametrize("module_name, class_name", ANALYZER_MODULES)
    def test_production_and_staging_thumbprints_differ_when_uris_differ(
        self, module_name: str, class_name: str
    ):
        module_under_test = import_module(f"app.domain.analyzers.{module_name}")
        input_uris = getattr(module_under_test, "INPUT_URIS")
        class_under_test = getattr(module_under_test, class_name)

        production = class_under_test(
            compute_engine=None, input_uris=input_uris[Environment.production]
        )
        staging = class_under_test(
            compute_engine=None,
            input_uris={
                **input_uris[Environment.production],
                "some_zarr_uri": "s3://new/path.zarr",
            },
        )

        # Only meaningful if staging has distinct URIs; if identical, they should match
        if input_uris[Environment.production] != input_uris[Environment.staging]:
            assert production.thumbprint() != staging.thumbprint()
        else:
            assert production.thumbprint() == staging.thumbprint()
