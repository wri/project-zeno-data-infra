import pytest

from app.analysis.dist_alerts.analysis import zonal_statistics
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.dist_alerts import (
    DistAlertsAnalytics,
    DistAlertsAnalyticsIn,
)

from app.analysis.common.analysis import get_geojson


class TestNaturalLandsServiceCompute:

    @pytest.mark.asyncio
    async def test_compute_returns_correct_status(
        self
    ):
        geojson = {
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
        }

        resource: DistAlertsAnalytics = await zonal_statistics(
            aoi={
                "type": "indigenous_land",
                "ids": ["1918"]
            },
            geojson=geojson,
            intersection="driver",
        )
        print(resource)
