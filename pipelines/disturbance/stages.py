from typing import Tuple, Optional
import xarray as xr

ExpectedGroupsType = Tuple

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


