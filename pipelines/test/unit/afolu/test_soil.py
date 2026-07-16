import pandas as pd

from pipelines.afolu import stages


def test_mineral_expansion_broadcasts_one_period_to_all_years():
    # a reduce over the 5 SOC interval indices; only period 3 should survive and
    # broadcast to every annual year (mineral's reference behaviour).
    df = pd.DataFrame(
        {"year": [0, 1, 2, 3, 4], "net_flux_MgCO2e": [10.0, 20.0, 30.0, 40.0, 50.0]}
    )
    out = stages._to_calendar_years(df, stages.MINERAL_YEAR_EXPANSION)
    assert sorted(out["year"]) == list(range(2016, 2025))
    assert (out["net_flux_MgCO2e"] == 40.0).all()  # only period index 3


def test_two_period_expansion_maps_each_period_to_its_year_range():
    df = pd.DataFrame({"year": [2020, 2024], "v": [1.0, 2.0]})
    out = stages._to_calendar_years(
        df, {2020: [2016, 2017, 2018, 2019, 2020], 2024: [2021, 2022, 2023, 2024]}
    )
    assert set(out[out.v == 1.0]["year"]) == {2016, 2017, 2018, 2019, 2020}
    assert set(out[out.v == 2.0]["year"]) == {2021, 2022, 2023, 2024}


def test_no_expansion_is_calendar_offset():
    df = pd.DataFrame({"year": [0, 8], "v": [1.0, 2.0]})
    out = stages._to_calendar_years(df, None)
    assert sorted(out["year"]) == [2016, 2024]
