import os

import pytest

from app.infrastructure.external_services.data_api_compute_service import (
    DataApiComputeService,
)


class TestDataApiComputeService:
    @pytest.mark.asyncio
    async def test_compute(self):
        data_api_compute_service = DataApiComputeService(
            api_key=os.environ.get("API_KEY")
        )
        result = await data_api_compute_service.compute(
            {
                "dataset": "gadm__tcl__iso_change",
                "version": "v20250515",
                "query": (
                    "SELECT iso, umd_tree_cover_loss__year, SUM(umd_tree_cover_loss__ha) AS umd_tree_cover_loss__ha, "
                    'SUM("gfw_gross_emissions_co2e_all_gases__Mg") AS '
                    '"gfw_gross_emissions_co2e_all_gases__Mg" FROM data WHERE '
                    "umd_tree_cover_density_2000__threshold = 30 AND iso in ('BRA') AND "
                    "is__ifl_intact_forest_landscapes_2000 = true GROUP BY iso, "
                    "umd_tree_cover_loss__year"
                ),
            }
        )

        assert list(result[0].keys()) == [
            "iso",
            "umd_tree_cover_loss__year",
            "umd_tree_cover_loss__ha",
            "gfw_gross_emissions_co2e_all_gases__Mg",
        ]
