from unittest.mock import MagicMock, patch

from pipelines.prefect_flows.common_stages import create_zarr_from_tiles

ZARR_URI = "s3://bucket/output.zarr"
TILES_GEOJSON_URI = "s3://bucket/tiles.geojson"
TILE_URIS = ["s3://bucket/tile_0.tif", "s3://bucket/tile_1.tif"]
OTF_CHUNK_SIZE = 256
PIPELINE_CHUNK_SIZE = 4096


@patch("pipelines.prefect_flows.common_stages.s3_uri_exists", return_value=False)
@patch("pipelines.prefect_flows.common_stages._get_tile_uris", return_value=TILE_URIS)
@patch("pipelines.prefect_flows.common_stages.xr.open_mfdataset")
def test_writes_multiple_groups(mock_open_mfdataset, mock_get_tiles, mock_s3_exists):
    """Writes multiple groups, each rechunked to its own chunk size, in a single open_mfdataset call."""
    mock_dataset = MagicMock()
    persisted_dataset = mock_dataset.persist.return_value
    mock_open_mfdataset.return_value = mock_dataset

    result = create_zarr_from_tiles(
        TILES_GEOJSON_URI,
        ZARR_URI,
        groups=[(OTF_CHUNK_SIZE, "otf"), (PIPELINE_CHUNK_SIZE, "pipeline")],
    )

    assert result == ZARR_URI
    # Dataset is opened once with the largest chunk size
    mock_open_mfdataset.assert_called_once_with(
        TILE_URIS,
        parallel=True,
        chunks={"x": PIPELINE_CHUNK_SIZE, "y": PIPELINE_CHUNK_SIZE},
    )
    # Each group is rechunked to its own size before writing (on the persisted dataset)
    persisted_dataset.chunk.assert_any_call({"x": OTF_CHUNK_SIZE, "y": OTF_CHUNK_SIZE})
    persisted_dataset.chunk.assert_any_call(
        {"x": PIPELINE_CHUNK_SIZE, "y": PIPELINE_CHUNK_SIZE}
    )
    # to_zarr is called on the rechunked dataset, not the original
    rechunked = persisted_dataset.chunk.return_value
    assert rechunked.to_zarr.call_count == 2
    rechunked.to_zarr.assert_any_call(ZARR_URI, mode="w", group="otf")
    rechunked.to_zarr.assert_any_call(ZARR_URI, mode="w", group="pipeline")
