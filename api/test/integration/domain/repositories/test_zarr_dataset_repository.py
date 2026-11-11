import pytest

from app.domain.models.dataset import Dataset
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository

pytestmark = pytest.mark.integration

# Integrated alerts reads the 10m pixel area zarr directly, not through otf.
ON_THE_FLY_DATASETS = [
    dataset
    for dataset in ZarrDatasetRepository._ZARR_URIS[Environment.production]
    if dataset != Dataset.pixel_area_m2_10m
]


@pytest.mark.parametrize("dataset", ON_THE_FLY_DATASETS, ids=str)
def test_every_production_zarr_opens_for_on_the_fly_reads(dataset):
    # On-the-fly reads expect an otf group in each zarr; a zarr missing it only
    # fails once a request touches that dataset.
    data = ZarrDatasetRepository(Environment.production).open_source(dataset)

    assert data.size > 0
