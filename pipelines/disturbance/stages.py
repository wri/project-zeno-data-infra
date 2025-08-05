from typing import Tuple, Optional
import xarray as xr
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import date

from pipelines.prefect_flows.common_stages import create_result_dataframe as common_create_result_dataframe

ExpectedGroupsType = Tuple

alerts_confidence = {
    2: "low",
    3: "high"
}


def setup_compute(
    datasets: Tuple[xr.Dataset, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xrarray reduce on dist alerts"""
    dist_alerts, country, region, subregion, contextual_layer = datasets

    mask = dist_alerts.confidence
    groupbys: Tuple[xr.Dataset, ...] = (
        country.band_data.rename("country"),
        region.band_data.rename("region"),
        subregion.band_data.rename("subregion"),
        dist_alerts.alert_date,
        dist_alerts.confidence,
    )
    if contextual_layer is not None:
        groupbys = (
            groupbys[:3]
            + (contextual_layer.band_data.rename(contextual_column_name),)
            + groupbys[3:]
        )

    return (mask, groupbys, expected_groups)


def create_result_dataframe(alerts_count: xr.Dataset) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_count)
    df.rename(columns={'value': 'count'}, inplace=True)
    df.rename(columns={'confidence': 'alert_confidence'}, inplace=True)
    df['alert_date'] = df.sort_values(by='alert_date').alert_date.apply(lambda x: date(2020, 12, 31) + relativedelta(days=x))
    df['alert_confidence'] = df.alert_confidence.apply(lambda x: alerts_confidence[x])
    return df
