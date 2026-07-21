"""QC for the Land GHG inventory vegetation precompute.

Compares this run's country-level totals against a reference dataset of the same
shape (``country, land_state_class, <measures>``). A (country, land_state_class)
total is flagged only when it differs by BOTH more than ``REL_THRESHOLD`` and more
than ``ABS_THRESHOLD`` MgCO2e, so tiny-magnitude noise (and 10 m coastline
differences) don't trip it. This module knows nothing about where the reference
came from — that lives behind ``reference.load_reference_totals``.
"""

import pandas as pd

from pipelines.land_ghg_inventory.stages import MEASURES
from pipelines.prefect_flows.common_stages import symmetric_relative_difference

REL_THRESHOLD = 0.02
ABS_THRESHOLD = 1000.0  # MgCO2e


def qc_against_reference(
    result_df: pd.DataFrame,
    reference_totals: pd.DataFrame,
    rel_threshold: float = REL_THRESHOLD,
    abs_threshold: float = ABS_THRESHOLD,
) -> bool:
    """Return True if every country x land_state_class total matches the
    reference (within the dual threshold)."""
    keys = ["country", "land_state_class"]
    country_rows = result_df[~result_df["aoi_id"].str.contains(".", regex=False)]
    ours = (
        country_rows.rename(columns={"aoi_id": "country"})
        .groupby(keys)[MEASURES]
        .sum()
        .reset_index()
    )
    merged = ours.merge(reference_totals, on=keys, suffixes=("_ours", "_ref"))

    passed = True
    for measure in MEASURES:
        for row in merged.itertuples():
            ours_value = getattr(row, f"{measure}_ours")
            ref_value = getattr(row, f"{measure}_ref")
            relative = symmetric_relative_difference(ours_value, ref_value)
            absolute = abs(ours_value - ref_value)
            if relative > rel_threshold and absolute > abs_threshold:
                passed = False
                print(
                    f"QC FLAG {row.country}/{row.land_state_class}/"
                    f"{measure}: ours={ours_value:.1f} ref={ref_value:.1f} "
                    f"rel={relative:.3f}"
                )
    return passed
