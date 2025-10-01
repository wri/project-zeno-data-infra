from functools import partial

import pandas as pd


class DaskAoiMapService:
    def __init__(self, dask_client, aoi_geometry_repository, dataset_repository):
        self.dask_client = dask_client
        self.aoi_geometry_repository = aoi_geometry_repository
        self.dataset_repository = dataset_repository

    async def map(self, aoi_ids, aoi_type, map_function):
        aoi_geometries = await self.aoi_geometry_repository.load(aoi_type, aoi_ids)
        aoi_partial = partial(
            map_function,
            dataset_repository=self.dataset_repository,
        )
        futures = self.dask_client.map(aoi_partial, list(zip(aoi_ids, aoi_geometries)))
        results_per_aoi = await self.dask_client.gather(futures)

        results = pd.concat(results_per_aoi)

        results["aoi_type"] = aoi_type
        return results
