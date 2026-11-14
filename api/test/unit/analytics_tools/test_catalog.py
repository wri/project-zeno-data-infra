import pytest

from analytics_tools.catalog import ApiCatalog, UnknownDatasetError


def openapi_document():
    return {
        "paths": {
            "/v0/land_change/tree_cover_loss/analytics": {
                "post": {
                    "summary": "Create Tree Cover Loss Analysis Task",
                    "description": "Annual tree cover loss.",
                    "x-dataset": {"time_basis": "annual", "coverage": "2001-2025"},
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/TreeCoverLossIn"
                                }
                            }
                        }
                    },
                }
            },
            "/v0/land_change/tree_cover_loss/analytics/{resource_id}": {
                "get": {"summary": "Get Tree Cover Loss Analysis Result"}
            },
            "/v0/land_change/tree_cover/analytics": {
                "post": {"summary": "Create Tree Cover Analysis Task"}
            },
            "/": {"get": {"summary": "Read Root"}},
        },
        "components": {
            "schemas": {
                "TreeCoverLossIn": {
                    "type": "object",
                    "properties": {
                        "aoi": {"$ref": "#/components/schemas/AdminAreaOfInterest"},
                        "canopy_cover": {"type": "integer"},
                    },
                },
                "AdminAreaOfInterest": {
                    "type": "object",
                    "properties": {"ids": {"type": "array"}},
                },
            }
        },
    }


def test_lists_only_analysis_creation_endpoints_as_datasets():
    catalog = ApiCatalog(openapi_document())

    datasets = catalog.datasets()

    assert [dataset.name for dataset in datasets] == ["tree_cover_loss", "tree_cover"]
    assert datasets[0].title == "Create Tree Cover Loss Analysis Task"


def test_lists_comparability_facts_declared_by_each_dataset():
    catalog = ApiCatalog(openapi_document())

    loss, cover = catalog.datasets()

    assert loss.facts == {"time_basis": "annual", "coverage": "2001-2025"}
    assert cover.facts == {}


def test_describe_inlines_nested_schema_references():
    catalog = ApiCatalog(openapi_document())

    dataset = catalog.describe("tree_cover_loss")

    aoi_schema = dataset.request_schema["properties"]["aoi"]
    assert aoi_schema["properties"]["ids"] == {"type": "array"}
    assert dataset.description == "Annual tree cover loss."
    assert dataset.path == "/v0/land_change/tree_cover_loss/analytics"


def test_describe_unknown_dataset_names_the_available_ones():
    catalog = ApiCatalog(openapi_document())

    with pytest.raises(UnknownDatasetError, match="tree_cover_loss"):
        catalog.describe("tree_cover_density")
