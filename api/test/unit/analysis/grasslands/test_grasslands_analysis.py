import asyncio
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import rioxarray  # noqa: F401
import xarray as xr
from app.domain.analyzers.grasslands_analyzer import GrasslandsAnalyzer


class TestGrasslandsPreComputedAnalysis:
    @pytest.fixture
    def precomputed_gadm_results(self, tmp_path):
        data = [
            [2000, "BRA.1", 3.485880],
            [2001, "BRA.1", 3.864716],
            [2002, "BRA.1", 6.138082],
            [2003, "BRA.1", 6.516994],
            [2004, "BRA.1", 11.366896],
            [2005, "BRA.1", 9.775381],
            [2007, "BRA.1", 29.250175],
            [2006, "BRA.1", 36.524731],
            [2010, "BRA.1", 20.687592],
            [2009, "BRA.1", 5.986679],
            [2008, "BRA.1", 16.216671],
            [2015, "BRA.1", 29.325933],
            [2013, "BRA.1", 3.713125],
            [2014, "BRA.1", 5.531692],
            [2012, "BRA.1", 3.864668],
            [2011, "BRA.1", 17.125580],
            [2018, "BRA.1", 37.585121],
            [2019, "BRA.1", 45.466499],
            [2017, "BRA.1", 48.118423],
            [2016, "BRA.1", 55.847614],
            [2021, "BRA.1", 19.854221],
            [2022, "BRA.1", 96.844940],
            [2020, "BRA.1", 30.766319],
            [2003, "BRA.1.2", 0.075572],
            [2009, "BRA.1.2", 1.134664],
            [2012, "BRA.1.2", 0.151125],
            [2010, "BRA.1.2", 0.605054],
            [2011, "BRA.1.2", 0.151125],
            [2017, "BRA.1.2", 0.151219],
            [2008, "BRA.1.2", 0.378089],
            [2007, "BRA.1.2", 0.226913],
            [2004, "BRA.1.2", 0.075562],
            [2019, "BRA.1.2", 0.075663],
            [2018, "BRA.1.2", 0.151308],
            [2015, "BRA.1.2", 0.151168],
            [2020, "BRA.1.2", 0.151414],
            [2021, "BRA.1.2", 0.075655],
            [2022, "BRA.1.2", 0.378285],
        ]

        df = pd.DataFrame(
            data,
            columns=["year", "aoi_id", "area_ha"],
        )

        parquet_file = tmp_path / "grasslands_data.parquet"
        df.to_parquet(parquet_file, index=False)

        return parquet_file

    @pytest.mark.asyncio
    async def test_precomputed_zonal_stats_for_region(self, precomputed_gadm_results):
        gadm_id = "BRA.1"

        result_df = GrasslandsAnalyzer.analyze_admin_areas(
            [gadm_id], precomputed_gadm_results
        )
        print(result_df)

        # Aggregated yearly data
        data = [
            [2000, 3.485880],
            [2001, 3.864716],
            [2002, 6.138082],
            [2003, 6.516994],
            [2004, 11.366896],
            [2005, 9.775381],
            [2006, 36.524731],
            [2007, 29.250175],
            [2008, 16.216671],
            [2009, 5.986679],
            [2010, 20.687592],
            [2011, 17.125580],
            [2012, 3.864668],
            [2013, 3.713125],
            [2014, 5.531692],
            [2015, 29.325933],
            [2016, 55.847614],
            [2017, 48.118423],
            [2018, 37.585121],
            [2019, 45.466499],
            [2020, 30.766319],
            [2021, 19.854221],
            [2022, 96.844940],
        ]

        expected_df = pd.DataFrame(data, columns=["year", "area_ha"])

        expected_df["aoi_id"] = "BRA.1"
        expected_df["aoi_type"] = "admin"

        pd.testing.assert_frame_equal(
            expected_df,
            result_df,
            check_like=True,
            check_dtype=False,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )


class TestGrasslandsOTFAnalysis:
    @pytest.fixture
    def grasslands_datacube(self):
        years = np.arange(2000, 2023)
        y_vals = np.linspace(48.0, 47.99775, 10)
        x_vals = np.linspace(105.0, 105.00225, 10)

        values = np.tile(np.arange(4, dtype=np.uint8), 10 * 10 * len(years) // 4)
        data = values.reshape((len(years), 10, 10))

        band_data = xr.DataArray(
            data,
            coords={"year": years, "y": y_vals, "x": x_vals},
            dims=["year", "y", "x"],
            name="band_data",
        )

        return xr.Dataset({"band_data": band_data})

    @pytest.fixture
    def pixel_area(self):
        # Define the base column (along y) — 10 values
        column_vals = np.array(
            [
                518.6011,
                518.6036,
                518.606,
                518.6085,
                518.611,
                518.61346,
                518.61597,
                518.61847,
                518.6209,
                518.6234,
            ],
            dtype=np.float32,
        )

        y_vals = np.linspace(48.0, 47.99775, 10)  # latitude
        x_vals = np.linspace(105.0, 105.00225, 10)  # longitude

        areas_2d = np.tile(column_vals[:, np.newaxis], (1, 10))
        areas_3d = areas_2d[np.newaxis, :, :]  # shape: (1, 10, 10)
        pixel_area = xr.DataArray(
            areas_3d,
            coords={"band": [1], "y": y_vals, "x": x_vals},
            dims=["band", "y", "x"],
            name="band_data",
        )

        ds = xr.Dataset({"band_data": pixel_area})
        return ds

    @pytest.mark.asyncio
    @patch("app.analysis.common.analysis.read_zarr")
    async def test_grasslands_otf_analysis(
        self, mock_read_zarr, grasslands_datacube, pixel_area
    ):
        mock_read_zarr.side_effect = [grasslands_datacube, pixel_area]

        aoi = {
            "type": "Feature",
            "properties": {"id": "test_aoi"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [105.0006, 47.9987],
                        [105.0016, 47.9987],
                        [105.0016, 47.9978],
                        [105.0006, 47.9978],
                        [105.0006, 47.9987],
                    ]
                ],
            },
        }

        result_df = GrasslandsAnalyzer.analyze_area(aoi, aoi["geometry"], 2000, 2022)

        loop = asyncio.get_event_loop()
        computed_df = await loop.run_in_executor(None, result_df.compute)

        years = np.arange(2000, 2023)
        expected_df = pd.DataFrame(
            {
                "year": years,
                "area_ha": np.array(([1555.85522] * len(years))).astype(np.float32),
                "aoi_type": ["feature"] * len(years),
                "aoi_id": ["test_aoi"] * len(years),
            }
        )

        pd.testing.assert_frame_equal(
            expected_df,
            computed_df,
            check_like=True,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )
