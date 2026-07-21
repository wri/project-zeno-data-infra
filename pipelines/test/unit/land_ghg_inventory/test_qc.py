import pandas as pd

from pipelines.land_ghg_inventory.qc import qc_against_reference


def reference_totals():
    return pd.DataFrame(
        {
            "country": ["STP", "STP"],
            "land_state_class": ["tree_loss", "trees_remaining_trees"],
            "gross_emissions_MgCO2e": [9182.0, 1244.5],
            "gross_removals_MgCO2": [0.0, -11663944.0],
            "net_flux_MgCO2e": [9182.0, -11663944.0],
        }
    )


def result_df(scale=1.0):
    # country-level rows (aoi_id without a dot) plus a sub-admin row that the
    # country-level QC must ignore.
    return pd.DataFrame(
        {
            "aoi_id": ["STP", "STP", "STP.1"],
            "aoi_type": ["admin"] * 3,
            "land_state_class": [
                "tree_loss",
                "trees_remaining_trees",
                "tree_loss",
            ],
            "year": [2016, 2016, 2016],
            "gross_emissions_MgCO2e": [9182.0 * scale, 1244.5 * scale, 999.0],
            "gross_removals_MgCO2": [0.0, -11663944.0 * scale, 0.0],
            "net_flux_MgCO2e": [9182.0 * scale, -11663944.0 * scale, 999.0],
        }
    )


def test_qc_passes_when_country_totals_match():
    assert qc_against_reference(result_df(1.0), reference_totals()) is True


def test_qc_fails_on_large_relative_and_absolute_diff():
    assert qc_against_reference(result_df(2.0), reference_totals()) is False


def test_qc_ignores_tiny_absolute_diff():
    # large % difference but tiny absolute magnitude -> not flagged
    reference = pd.DataFrame(
        {
            "country": ["STP"],
            "land_state_class": ["tree_gain"],
            "gross_emissions_MgCO2e": [1.0],
            "gross_removals_MgCO2": [-10.0],
            "net_flux_MgCO2e": [-9.0],
        }
    )
    result = pd.DataFrame(
        {
            "aoi_id": ["STP"],
            "aoi_type": ["admin"],
            "land_state_class": ["tree_gain"],
            "year": [2016],
            "gross_emissions_MgCO2e": [5.0],
            "gross_removals_MgCO2": [-50.0],
            "net_flux_MgCO2e": [-45.0],
        }
    )
    assert qc_against_reference(result, reference) is True
