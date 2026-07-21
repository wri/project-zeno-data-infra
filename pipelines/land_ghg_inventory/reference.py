"""Adapter for an external reference dataset used only to QC a pipeline run.

The reference is an independently-produced admin-level zonal-statistics table. Its
source and schema are provisional and may be swapped, so the rest of the pipeline
depends only on the tidy shape this returns — ``country, land_state_class,
<measures>`` — never on where the numbers came from. To point QC at a different
source, replace this adapter (and the column maps) without touching ``qc`` or the
flow logic.
"""

import pandas as pd

from pipelines.land_ghg_inventory.land_state_categories import (
    VEGETATION_CATEGORIES,
    classify,
)
from pipelines.land_ghg_inventory.stages import MEASURES

# our measure name -> reference column
VEGETATION_SOURCE_COLUMNS = {
    "gross_emissions_MgCO2e": "veg__gross_emissions__all_C_pools__all_gases__MgCO2e_yr",
    "gross_removals_MgCO2": "veg__gross_removals__all_C_pools__MgCO2_yr",
    "net_flux_MgCO2e": "veg__net_flux__all_C_pools__all_gases__MgCO2e_yr",
}


def load_reference_totals(reference_uri: str) -> pd.DataFrame:
    """Country x land_state_class totals (summed over years) from the reference
    source, in the same tidy shape the vegetation pipeline produces."""
    reference = pd.read_parquet(
        reference_uri,
        columns=[
            "adm0",
            "land_state_detailed_class",
            "land_state_broad_class",
            *VEGETATION_SOURCE_COLUMNS.values(),
        ],
    )
    return _vegetation_totals(reference)


def _vegetation_totals(reference: pd.DataFrame) -> pd.DataFrame:
    veg = reference.rename(
        columns={source: name for name, source in VEGETATION_SOURCE_COLUMNS.items()}
    ).copy()
    veg["land_state_class"] = [
        VEGETATION_CATEGORIES[classify(detailed, broad)]
        for detailed, broad in zip(
            veg["land_state_detailed_class"], veg["land_state_broad_class"]
        )
    ]
    veg = veg[veg["land_state_class"] != "excluded"]
    totals = veg.groupby(["adm0", "land_state_class"])[MEASURES].sum().reset_index()
    return totals.rename(columns={"adm0": "country"})
