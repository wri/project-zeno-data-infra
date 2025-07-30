import pandas as pd
import pytest
from unittest.mock import patch


from app.analysis.grasslands.analysis import get_precomputed_statistic_on_gadm_aoi


class TestGrasslandsPreComputedAnalysis:

    @pytest.fixture
    def precomputed_gadm_results(self, tmp_path):

        data = [
            [2000, "BRA", 1, 1, 3.485880],
            [2001, "BRA", 1, 1, 3.864716],
            [2002, "BRA", 1, 1, 6.138082],
            [2003, "BRA", 1, 1, 6.516994],
            [2004, "BRA", 1, 1, 11.366896],
            [2005, "BRA", 1, 1, 9.775381],
            [2007, "BRA", 1, 1, 29.250175],
            [2006, "BRA", 1, 1, 36.524731],
            [2010, "BRA", 1, 1, 20.687592],
            [2009, "BRA", 1, 1, 5.986679],
            [2008, "BRA", 1, 1, 16.216671],
            [2015, "BRA", 1, 1, 29.325933],
            [2013, "BRA", 1, 1, 3.713125],
            [2014, "BRA", 1, 1, 5.531692],
            [2012, "BRA", 1, 1, 3.864668],
            [2011, "BRA", 1, 1, 17.125580],
            [2018, "BRA", 1, 1, 37.585121],
            [2019, "BRA", 1, 1, 45.466499],
            [2017, "BRA", 1, 1, 48.118423],
            [2016, "BRA", 1, 1, 55.847614],
            [2021, "BRA", 1, 1, 19.854221],
            [2022, "BRA", 1, 1, 96.844940],
            [2020, "BRA", 1, 1, 30.766319],
            [2003, "BRA", 1, 2, 0.075572],
            [2009, "BRA", 1, 2, 1.134664],
            [2012, "BRA", 1, 2, 0.151125],
            [2010, "BRA", 1, 2, 0.605054],
            [2011, "BRA", 1, 2, 0.151125],
            [2017, "BRA", 1, 2, 0.151219],
            [2008, "BRA", 1, 2, 0.378089],
            [2007, "BRA", 1, 2, 0.226913],
            [2004, "BRA", 1, 2, 0.075562],
            [2019, "BRA", 1, 2, 0.075663],
            [2018, "BRA", 1, 2, 0.151308],
            [2015, "BRA", 1, 2, 0.151168],
            [2020, "BRA", 1, 2, 0.151414],
            [2021, "BRA", 1, 2, 0.075655],
            [2022, "BRA", 1, 2, 0.378285],
        ]

        df = pd.DataFrame(
            data, columns=["year", "country", "region", "subregion", "grassland_area"]
        )

        parquet_file = tmp_path / "grasslands_data.parquet"
        df.to_parquet(parquet_file, index=False)

        return parquet_file

    @pytest.mark.asyncio
    async def test_precomputed_zonal_stats_for_region(self, precomputed_gadm_results):

        gadm_id = "BRA.1"

        result_df = await get_precomputed_statistic_on_gadm_aoi(
            gadm_id, precomputed_gadm_results
        )

        # Aggregated yearly data
        data = [
            [2000, 3.485880],
            [2001, 3.864716],
            [2002, 6.138082],
            [2003, 6.592566],
            [2004, 11.442458],
            [2005, 9.775381],
            [2006, 36.524731],
            [2007, 29.477088],
            [2008, 16.594760],
            [2009, 7.121343],
            [2010, 21.292646],
            [2011, 17.276705],
            [2012, 4.015793],
            [2013, 3.713125],
            [2014, 5.531692],
            [2015, 29.477101],
            [2016, 55.847614],
            [2017, 48.269642],
            [2018, 37.736429],
            [2019, 45.542162],
            [2020, 30.917733],
            [2021, 19.929876],
            [2022, 97.223225],
        ]

        expected_df = pd.DataFrame(data, columns=["year", "grassland_area"])

        expected_df["aoi_id"] = "BRA.1"
        expected_df["aoi_type"] = "admin"

        pd.testing.assert_frame_equal(expected_df, result_df, check_like=True)
