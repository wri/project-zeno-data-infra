import pytest
from unittest.mock import patch

from pandera.pandas import DataFrameSchema, Column, Check, dtypes
from prefect.testing.utilities import prefect_test_harness

from pipelines.natural_lands.prefect_flows.nl_flow import gadm_natural_lands_area


@pytest.mark.integration
@pytest.mark.slow
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.prefect_flows.common_stages._load_zarr")
def test_gadm_area_by_natural_lands_result(
    mock_load_zarr,
    mock_save_parquet,
    area_ds,
    country_ds,
    region_ds,
    subregion_ds,
    natural_lands_ds,
):
    alert_schema = DataFrameSchema(
        name="GADM Dist Alerts",
        columns={
            "country": Column(int, Check.ge(0)),
            "region": Column(int, Check.ge(0)),
            "subregion": Column(int, Check.ge(0)),
            "natural_lands": Column(int, Check.ge(0)),
            "value": Column(dtypes.Float32, Check.in_range(0.0, 2300.0)),
        },
        unique=[
            "country",
            "region",
            "subregion",
            "natural_lands",
        ],
        checks=Check(
            lambda df: (
                df.groupby(
                    ["country", "region", "subregion", "natural_lands"]
                )["value"].transform("nunique")
                == 1
            ),
            name="two_confidences_per_group",
            error="Each location-date must have exactly 1 confidence levels",
        ),
    )

    mock_load_zarr.side_effect = [
        area_ds,
        country_ds,
        region_ds,
        subregion_ds,
        natural_lands_ds,
    ]

    with prefect_test_harness():
        result_uri = gadm_natural_lands_area(
            #  dist_zarr_uri="s3://dummy_zarr_uri",
            #  dist_version="test_v1",
            overwrite=True
        )

    assert (
        result_uri
        == "s3://gfw-data-lake/sbtn_natural_lands/tabular/zonal_stats/gadm/gadm_adm2.parquet"
    )

    # Verify
    result = mock_save_parquet.call_args[0][0]
    print(f"\nGADM dist alerts by natural lands result:\n{result}")
    alert_schema.validate(result)
