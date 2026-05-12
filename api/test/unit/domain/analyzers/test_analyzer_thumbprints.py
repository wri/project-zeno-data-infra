import uuid
from typing import Dict

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis


class TestedAnalyzer(Analyzer):
    def __init__(
        self,
        input_uris: Dict[str, str] | None = None,
    ):
        self.input_uris = input_uris

    async def analyze(self, analysis: Analysis) -> None:
        return None


class TestAnalyzerThumbprints:
    def test_thumbprint_stable_with_same_input_uris(self):
        input_uris = {"foo": "bar"}

        analyzer_a = TestedAnalyzer(input_uris=input_uris)
        analyzer_b = TestedAnalyzer(input_uris=input_uris)

        assert analyzer_a.thumbprint() == analyzer_b.thumbprint()
        assert isinstance(analyzer_a.thumbprint(), uuid.UUID)

    def test_thumbprint_changes_when_input_uris_change(self):
        """Simulates deploying new data: the resource ID must change so stale
        cached results are not served."""
        input_uris = {"foo": "bar"}
        analyzer_a = TestedAnalyzer(input_uris=input_uris)
        analyzer_b = TestedAnalyzer(
            input_uris={
                **input_uris,
                "some_zarr_uri": "s3://new/path.zarr",
            },
        )

        assert analyzer_a.thumbprint() != analyzer_b.thumbprint()
