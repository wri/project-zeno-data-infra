import numpy as np
import xarray as xr

from app.domain.compute_engines.handlers.otf_implementations.flox_otf_handler import (
    FloxOTFHandler,
)
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)

COORDS = {"x": np.arange(10, dtype=float), "y": np.arange(9, -1, -1, dtype=float)}
# simulates zarrs whose pixel centers differ only by floating point noise
OFFSET_COORDS = {k: v + 1e-9 for k, v in COORDS.items()}


class StubDatasetRepository:
    def load(self, dataset, geometry=None):
        if dataset == Dataset.area_hectares:
            data, coords = np.full((10, 10), 5000.0), COORDS
        elif dataset == Dataset.canopy_cover:
            # left half is 1s, right half is 5s
            data = np.hstack([np.ones((10, 5)), np.full((10, 5), 5)])
            coords = OFFSET_COORDS
        else:
            raise ValueError(f"Not a valid dataset for this test:{dataset}")

        return xr.DataArray(
            data, coords=coords, dims=("x", "y"), name=dataset.get_field_name()
        )

    def translate(self, dataset, value):
        return value


def test_filter_layer_is_reindexed_to_aggregate_layer():
    query = DatasetQuery(
        aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
        group_bys=[],
        filters=[DatasetFilter(dataset=Dataset.canopy_cover, op=">=", value=5)],
    )

    results = FloxOTFHandler._handle(
        ("1234", None),
        query=query,
        dataset_repository=StubDatasetRepository(),
        expected_groups_per_dataset={},
    )

    # only the right half (50 pixels) passes the canopy cover filter
    assert results["area_ha"].tolist() == [50 * 5000.0]
