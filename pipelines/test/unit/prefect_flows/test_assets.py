from pathlib import Path

from pipelines.prefect_flows.assets import result_asset_key, source_asset
from pipelines.sources.source_catalog import Source


def test_source_asset_is_keyed_by_the_data_it_reads():
    asset = source_asset(
        Source(
            uri="s3://bucket/vegetation.zarr",
            title="Vegetation flux",
            description="Annual vegetation flux.",
            url="https://bucket.s3.amazonaws.com/vegetation.zarr",
        )
    )

    assert asset.key == "s3://bucket/vegetation.zarr"
    assert asset.properties.name == "Vegetation flux"
    assert asset.properties.url == "https://bucket.s3.amazonaws.com/vegetation.zarr"


def test_result_asset_key_keeps_remote_uris():
    assert (
        result_asset_key("s3://bucket/result.parquet") == "s3://bucket/result.parquet"
    )


def test_result_asset_key_gives_local_paths_a_file_scheme():
    assert (
        result_asset_key("result.parquet") == Path("result.parquet").resolve().as_uri()
    )
