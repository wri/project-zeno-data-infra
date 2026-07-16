"""QC for the AFOLU vegetation precompute.

Compares this run's country-level totals against a reference dataset of the same
shape (``country, veg_category, <measures>``). A (country, vegetation-category)
total is flagged only when it differs by BOTH more than ``REL_THRESHOLD`` and more
than ``ABS_THRESHOLD`` MgCO2e, so tiny-magnitude noise (and 10 m coastline
differences) don't trip it. This module knows nothing about where the reference
came from — that lives behind ``reference.load_reference_totals``.
"""

import pandas as pd

from pipelines.afolu_vegetation.stages import MEASURES
from pipelines.prefect_flows.common_stages import symmetric_relative_difference

REL_THRESHOLD = 0.02
ABS_THRESHOLD = 1000.0  # MgCO2e


def qc_against_reference(
    result_df: pd.DataFrame,
    reference_totals: pd.DataFrame,
    rel_threshold: float = REL_THRESHOLD,
    abs_threshold: float = ABS_THRESHOLD,
) -> bool:
    """Return True if every country x category total matches the reference."""
    country_rows = result_df[~result_df["aoi_id"].str.contains(".", regex=False)]
    ours = (
        country_rows.groupby(["aoi_id", "veg_category"])[MEASURES]
        .sum()
        .reset_index()
        .rename(columns={"aoi_id": "country"})
    )
    merged = ours.merge(
        reference_totals, on=["country", "veg_category"], suffixes=("_ours", "_ref")
    )

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
                    f"QC FLAG {row.country}/{row.veg_category}/{measure}: "
                    f"ours={ours_value:.1f} ref={ref_value:.1f} rel={relative:.3f}"
                )
    return passed
