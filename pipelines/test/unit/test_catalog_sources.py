import json
from pathlib import Path

import pytest

from pipelines.catalog_sources import source_uri

REPOSITORY_CATALOG = Path(__file__).parents[2] / "catalog"


@pytest.fixture
def catalog_root(tmp_path):
    collection = {
        "type": "Collection",
        "id": "vegetation",
        "assets": {
            "data": {
                "href": "https://bucket.s3.amazonaws.com/vegetation.zarr",
                "alternate": {"s3": {"href": "s3://bucket/vegetation.zarr"}},
            }
        },
    }
    (tmp_path / "vegetation").mkdir()
    (tmp_path / "vegetation" / "collection.json").write_text(json.dumps(collection))
    return str(tmp_path)


def test_resolves_the_s3_alternate_rather_than_the_https_href(catalog_root):
    assert source_uri(catalog_root, "vegetation") == "s3://bucket/vegetation.zarr"


def test_unknown_collection_fails(catalog_root):
    with pytest.raises(FileNotFoundError):
        source_uri(catalog_root, "missing")


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
    assert source_uri(str(REPOSITORY_CATALOG), collection_id).startswith("s3://")
