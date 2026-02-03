from unittest.mock import MagicMock, patch

import pandas as pd
from shapely.geometry import box

from pipelines.tree_cover_loss.stages import TreeCoverLossTasks


def _make_mock_ee(get_info_value):
    image = MagicMock(name="ee_image")
    image.select.return_value = image
    image.selfMask.return_value = image
    image.gt.return_value = image
    image.updateMask.return_value = image
    image.multiply.return_value = image
    image.eq.return_value = image
    image.addBands.return_value = image
    image.rename.return_value = image
    image.projection.return_value.getInfo.return_value = {
        "crs": "EPSG:4326",
        "transform": [1, 0, 0, 0, 1, 0],
    }

    pixel_area = MagicMock(name="ee_pixel_area")
    pixel_area.divide.return_value = pixel_area

    feature = MagicMock(name="ee_feature")
    feature.copyProperties.return_value = feature

    fc = MagicMock(name="ee_feature_collection")
    country = MagicMock(name="ee_country")
    fc.filter.return_value = country
    fc_mapped = MagicMock(name="ee_fc_mapped")
    fc_mapped.getInfo.return_value = get_info_value
    country.map.return_value = fc_mapped

    reducer = MagicMock(name="ee_reducer")
    reducer.unweighted.return_value = reducer

    mock_ee = MagicMock(name="ee_module")
    mock_ee.Image.return_value = image
    mock_ee.Image.pixelArea.return_value = pixel_area
    mock_ee.FeatureCollection.return_value = fc
    mock_ee.Filter.eq.return_value = MagicMock(name="ee_filter")
    mock_ee.Reducer.sum.return_value = reducer
    mock_ee.Feature.return_value = feature

    return mock_ee, country, fc_mapped


def test_get_validation_statistics_uses_injected_ee_module():
    expected = {"features": [{"properties": {"area_ha": 1.0}}]}
    mock_ee, country, fc_mapped = _make_mock_ee(expected)

    result = TreeCoverLossTasks.get_validation_statistics(
        "BRB", ee_module=mock_ee, initialize=False
    )

    assert result == expected
    mock_ee.Initialize.assert_not_called()
    mock_ee.Image.assert_any_call("UMD/hansen/global_forest_change_2024_v1_12")
    mock_ee.FeatureCollection.assert_called_once()
    mock_ee.Filter.eq.assert_called_once_with("GID_0", "BRB")
    country.map.assert_called_once()
    fc_mapped.getInfo.assert_called_once()


def test_tcl_validation_flow():
    with patch(
        "pipelines.tree_cover_loss.stages.TreeCoverLossTasks.get_sample_statistics"
    ) as mock_sample, patch(
        "pipelines.tree_cover_loss.stages.TreeCoverLossTasks.get_validation_statistics"
    ) as mock_validation:
        mock_sample.return_value = pd.DataFrame({"area_ha": [100.0, 200.0]})
        mock_validation.return_value = pd.DataFrame({"area_ha": [100.0, 200.0]})

        assert TreeCoverLossTasks.qc_against_validation_source() is True

        mock_validation.return_value = pd.DataFrame({"area_ha": [100.0, 150.0]})
        assert TreeCoverLossTasks.qc_against_validation_source() is False


def test_get_sample_statistics_accepts_injected_geometry_lookup():
    custom_geometry = box(0, 0, 1, 1)
    expected = pd.DataFrame({"area_ha": [123.0]})
    geometry_lookup = MagicMock(return_value=custom_geometry)

    with patch("pipelines.tree_cover_loss.stages.umd_tree_cover_loss") as mock_flow:
        mock_flow.return_value = expected

        result = TreeCoverLossTasks.get_sample_statistics(
            "BRB", geometry_lookup=geometry_lookup
        )

        assert result is expected
        geometry_lookup.assert_called_once_with("BRB")
        mock_flow.assert_called_once()
        _, kwargs = mock_flow.call_args
        assert kwargs["bbox"] is custom_geometry
