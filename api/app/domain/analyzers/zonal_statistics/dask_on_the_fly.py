from functools import partial
from typing import Any, Callable, Dict

import dask.dataframe as dd

from app.analysis.common.analysis import get_geojson


class DaskOnTheFlyStatistics:
    """Computes zonal statistics per AOI on the fly across a Dask cluster.

    The dataset-specific pixel computation is the injected `area_fn`; the AOI
    preparation and the map/gather/compute orchestration are shared.
    """

    def __init__(
        self,
        compute_engine,
        input_uris: Dict[str, str],
        area_fn: Callable[..., "dd.DataFrame"],
        extract_params: Callable[[Any], Dict] = lambda _: {},
    ):
        self.compute_engine = compute_engine
        self.input_uris = input_uris
        self.area_fn = area_fn
        self.extract_params = extract_params

    async def compute(self, analytics_in) -> Dict[str, Any]:
        aois = analytics_in.aoi.model_dump()
        geojsons = await get_geojson(aois)

        if aois["type"] != "feature_collection":
            aoi_list = sorted(
                [{"type": aois["type"], "id": id} for id in aois["ids"]],
                key=lambda aoi: aoi["id"],
            )
        else:
            aoi_list = aois["feature_collection"]["features"]
            geojsons = [geojson["geometry"] for geojson in geojsons]

        area_partial = partial(
            self.area_fn, self.input_uris, **self.extract_params(analytics_in)
        )
        dd_df_futures = await self.compute_engine.gather(
            self.compute_engine.map(area_partial, aoi_list, geojsons)
        )
        dfs = await self.compute_engine.gather(dd_df_futures)
        combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
        return combined_results_df.to_dict(orient="list")
