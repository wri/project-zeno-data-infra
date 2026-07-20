"""Adapter for an external reference dataset used only to QC a pipeline run.

The reference is an independently-produced admin-level zonal-statistics table. Its
source and schema are provisional and may be swapped, so the rest of the pipeline
depends only on the tidy shape this returns — ``country, carbon_pool, flux_class,
<measures>`` — never on where the numbers came from. To point QC at a different
source, replace this adapter (and the column maps) without touching ``qc`` or the
flow logic.
"""

import pandas as pd

from pipelines.afolu.land_state_categories import VEGETATION_CATEGORIES, classify
from pipelines.afolu.stages import MEASURES

# our measure name -> reference column, per component
VEGETATION_SOURCE_COLUMNS = {
    "gross_emissions_MgCO2e": "veg__gross_emissions__all_C_pools__all_gases__MgCO2e_yr",
    "gross_removals_MgCO2": "veg__gross_removals__all_C_pools__MgCO2_yr",
    "net_flux_MgCO2e": "veg__net_flux__all_C_pools__all_gases__MgCO2e_yr",
}
MINERAL_SOURCE_COLUMNS = {
    "gross_emissions_MgCO2e": "SOC_loss__mineral_soil_extent__0_30cm_MgCO2_yr",
    "gross_removals_MgCO2": "SOC_gain__mineral_soil_extent__0_30cm_MgCO2_yr",
    "net_flux_MgCO2e": "SOC_net__mineral_soil_extent__0_30cm_MgCO2_yr",
}


def load_reference_totals(reference_uri: str) -> pd.DataFrame:
    """Country x (component, category) totals (summed over years) from the
    reference source, in the same tidy shape the pipeline produces."""
    reference = pd.read_parquet(
        reference_uri,
        columns=[
            "adm0",
            "land_state_detailed_class",
            "land_state_broad_class",
            *VEGETATION_SOURCE_COLUMNS.values(),
            *MINERAL_SOURCE_COLUMNS.values(),
        ],
    )
    return pd.concat(
        [_vegetation_totals(reference), _mineral_totals(reference)],
        ignore_index=True,
    )


def _vegetation_totals(reference: pd.DataFrame) -> pd.DataFrame:
    veg = reference.rename(
        columns={source: name for name, source in VEGETATION_SOURCE_COLUMNS.items()}
    ).copy()
    veg["flux_class"] = [
        VEGETATION_CATEGORIES[classify(detailed, broad)]
        for detailed, broad in zip(
            veg["land_state_detailed_class"], veg["land_state_broad_class"]
        )
    ]
    veg = veg[veg["flux_class"] != "excluded"]
    totals = veg.groupby(["adm0", "flux_class"])[MEASURES].sum().reset_index()
    totals["carbon_pool"] = "vegetation"
    return totals.rename(columns={"adm0": "country"})


def _mineral_totals(reference: pd.DataFrame) -> pd.DataFrame:
    mineral = reference.rename(
        columns={source: name for name, source in MINERAL_SOURCE_COLUMNS.items()}
    )
    totals = mineral.groupby("adm0")[MEASURES].sum().reset_index()
    totals["carbon_pool"] = "soil"
    totals["flux_class"] = "mineral"
    return totals.rename(columns={"adm0": "country"})
