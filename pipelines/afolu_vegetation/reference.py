"""Adapter for an external reference dataset used only to QC a pipeline run.

The reference is an independently-produced admin-level zonal-statistics table. Its
source and schema are provisional and may be swapped, so the rest of the pipeline
depends only on the tidy shape this returns — ``country, veg_category, <measures>`` —
never on where the numbers came from. To point QC at a different source, replace this
adapter (and ``SOURCE_MEASURE_COLUMNS``) without touching ``qc`` or the flow logic.
"""

import pandas as pd

from pipelines.afolu_vegetation.land_state_categories import (
    VEGETATION_CATEGORIES,
    classify,
)
from pipelines.afolu_vegetation.stages import MEASURES

# our measure name -> column name in the current reference source
SOURCE_MEASURE_COLUMNS = {
    "gross_emissions_MgCO2e": "veg__gross_emissions__all_C_pools__all_gases__MgCO2e_yr",
    "gross_removals_MgCO2": "veg__gross_removals__all_C_pools__MgCO2_yr",
    "net_flux_MgCO2e": "veg__net_flux__all_C_pools__all_gases__MgCO2e_yr",
}


def load_reference_totals(reference_uri: str) -> pd.DataFrame:
    """Country x vegetation-category totals (summed over years) from the reference
    source, bucketed with our classification rule."""
    reference = pd.read_parquet(
        reference_uri,
        columns=[
            "adm0",
            "land_state_detailed_class",
            "land_state_broad_class",
            *SOURCE_MEASURE_COLUMNS.values(),
        ],
    )
    reference["veg_category"] = [
        VEGETATION_CATEGORIES[classify(detailed, broad)]
        for detailed, broad in zip(
            reference["land_state_detailed_class"],
            reference["land_state_broad_class"],
        )
    ]
    reference = reference[reference["veg_category"] != "excluded"].rename(
        columns={source: name for name, source in SOURCE_MEASURE_COLUMNS.items()}
    )
    return (
        reference.groupby(["adm0", "veg_category"])[MEASURES]
        .sum()
        .reset_index()
        .rename(columns={"adm0": "country"})
    )
