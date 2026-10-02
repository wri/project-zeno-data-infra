import json
from pathlib import Path

import pytest

from pipelines.sources.portolan_catalog import PortolanCatalog
from pipelines.sources.source_catalog import Source

REPOSITORY_CATALOG = Path(__file__).parents[3] / "catalog"


@pytest.fixture
def catalog(tmp_path):
    collection = {
        "type": "Collection",
        "id": "vegetation",
        "title": "Vegetation flux",
        "description": "Annual vegetation flux.",
        "assets": {
            "data": {
                "href": "https://bucket.s3.amazonaws.com/vegetation.zarr",
                "alternate": {"s3": {"href": "s3://bucket/vegetation.zarr"}},
            }
        },
    }
    (tmp_path / "vegetation").mkdir()
    (tmp_path / "vegetation" / "collection.json").write_text(json.dumps(collection))
    return PortolanCatalog(str(tmp_path))


def test_source_reads_from_the_s3_alternate_and_links_the_https_href(catalog):
    assert catalog.source("vegetation") == Source(
        uri="s3://bucket/vegetation.zarr",
        title="Vegetation flux",
        description="Annual vegetation flux.",
        url="https://bucket.s3.amazonaws.com/vegetation.zarr",
    )


def test_unknown_source_fails(catalog):
    with pytest.raises(FileNotFoundError):
        catalog.source("missing")


@pytest.mark.parametrize(
    "collection_id",
    [
        "lulucf-vegetation",
        "mineral-soil",
        "organic-soil",
        "cropland-emissions",
        "livestock-emissions",
    ],
)
def test_repository_catalog_resolves_every_pipeline_source(collection_id):
    source = PortolanCatalog(str(REPOSITORY_CATALOG)).source(collection_id)
    assert source.uri.startswith("s3://")
