from enum import Enum
from typing import Any, List, Literal

import duckdb
import pandas as pd

from api.app.models.common.analysis import AnalyticsIn
from api.app.models.common.areas_of_interest import AreaOfInterest
from api.app.models.common.base import StrictBaseModel


class Dataset(Enum):
    tree_cover_loss = "tree_cover_loss"
    canopy_cover = "canopy_cover"
    area_hectares = "area_hectares"


class DatasetFilter(StrictBaseModel):
    dataset: Dataset
    op: Literal["=", "<", ">", "<=", ">=", "!="]
    value: Any


class DatasetAggregate(StrictBaseModel):
    dataset: Dataset
    func: Literal["sum", "count"]


class DatasetQuery(StrictBaseModel):
    aggregate: DatasetAggregate
    group_bys: List[Dataset]
    filters: List[DatasetFilter]


class PrecalcQueryService(StrictBaseModel):
    def __init__(self):
        pass

    async def execute(data_source: str, query: str) -> pd.DataFrame:
        return await duckdb.sql(query).df()


class PrecalcHandler:
    FIELDS = {
        Dataset.area_hectares: "area_ha",
        Dataset.tree_cover_loss: "loss_year",
        Dataset.canopy_cover: "canopy_cover",
    }

    def __init__(self, precalc_query_service):
        self.precalc_query_service = precalc_query_service

    async def handle(self, aoi_type, aoi_ids, query: DatasetQuery):
        if query.aggregate.dataset == Dataset.area_hectares and query.group_bys == [
            Dataset.tree_cover_loss
        ]:
            data_source = "s3://path/to/parquet"

        agg = f"{query.aggregate.func.upper()}({self.FIELDS[query.aggregate.dataset]}) AS {self.FIELDS[query.aggregate.dataset]}"
        groupby_fields = ", ".join(
            [self.FIELDS[dataset] for dataset in query.group_bys]
        )
        filters = " AND ".join(
            [
                f"{self.FIELDS[filt.dataset]} {filt.op} {str(filt.value)}"
                for filt in query.filters
            ]
        )
        filters += f" AND id in {tuple(aoi_ids)}"
        query = f"SELECT id, {groupby_fields}, {agg} FROM data_source WHERE {filters} GROUP BY id, {groupby_fields}"

        return await self.precalc_query_service.execute(data_source, query)


class FloxOTFHandler:
    DATA_ARRAYS = {
        Dataset.area_hectares: "s3://zar",
        Dataset.tree_cover_loss: "s3://zar",
        Dataset.canopy_cover: "s3://zarr",
    }
    EXPECTED_GROUPS = {}

    def __init__(self, area_list, precalc_query_service):
        self.precalc_query_service = precalc_query_service

    async def handle(self, query: DatasetQuery):
        by = self.DATA_ARRAYS[query.aggregate.dataset]
        func = query.aggregate.func

        objs = []
        expected_groups = []
        for filter in query.filters:
            da = self.DATA_ARRAYS[filter.dataset]
            filtered_da = da.apply_filter(filter.op, filter.value)
            objs.append(da)
            expected_groups.append(self.EXPECTED_GROUPS(filter.dataset))

        for group_by in query.group_bys:
            objs.append(self.DATA_ARRAYS[group_by.dataset])
            expected_groups.append(self.EXPECTED_GROUPS(group_by.dataset))

        results = xarray_reduce(by, *objs, func, expected_groups=expected_groups)


class ComputeEngine:
    def __init__(self, handler):
        self.handler = handler

    async def compute(self, aoi_type, aoi_ids, query: DatasetQuery):
        return await self.handler.handle(aoi_type, aoi_ids, query)


class AreaOfInterestList:
    def __init__(self, aois: AreaOfInterest, compute_engine: ComputeEngine):
        self.type = aois.type
        self.ids = aois.ids
        self.compute_engine = compute_engine

    async def get_tree_cover_loss(
        self, canopy_cover: int, start_year: int, end_year: int, forest_type: str
    ):
        query = DatasetQuery(
            aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
            group_bys=[Dataset.tree_cover_loss],
            filters=[
                DatasetFilter(
                    dataset=Dataset.canopy_cover, op=">=", value=canopy_cover
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op=">=",
                    value=start_year,
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op="<=",
                    value=end_year,
                ),
            ],
        )
        return await self.compute_engine.compute(self.type, self.ids, query)


class ParquetPrecalcHandler:
    FIELDS = {
        Dataset.area_hectares: "area_ha",
        Dataset.tree_cover_loss: "loss_year",
        Dataset.canopy_cover: "canopy_cover",
    }

    def __init__(self, precalc_query_service):
        self.precalc_query_service = precalc_query_service

    async def handle(self, areas: AreaOfInterestList, query: DatasetQuery):
        if query.aggregate.dataset == Dataset.area_hectares and query.group_bys == [
            Dataset.tree_cover_loss
        ]:
            data_source = "s3://path/to/parquet"

        agg = f"{query.aggregate.func.upper()}({self.FIELDS[query.aggregate.dataset]})"
        groupby_fields = ", ".join(
            [self.FIELDS[dataset] for dataset in query.group_bys]
        )
        filters = " AND ".join(
            [
                f"{self.FIELDS[filt.dataset]} {filt.op} {str(filt.value)}"
                for filt in query.filters
            ]
        )
        query = f"SELECT {groupby_fields}, {agg} FROM data_source WHERE {filters} GROUP BY {groupby_fields}"

        return await self.precalc_query_service.execute(query)


# class ParquetQueryService():
#     DATASET_PARQUETS = {
#         Dataset.tree_cover_loss: "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/tabular/zonal_stats/umd_tree_cover_loss_by_driver.parquet"
#     }

#     def __init__(self, dask_client):
#         self.dask_client = dask_client

#     async def execute(self, query: ParquetQuery):
#         # Dumbly doing this per request since the STS token expires eventually otherwise
#         # According to this issue, duckdb should auto refresh the token in 1.3.0,
#         # but it doesn't seem to work for us and people are reporting the same on the issue
#         # https://github.com/duckdb/duckdb-aws/issues/26
#         # TODO do this on lifecycle start once autorefresh works
#         duckdb.query(
#             """
#             CREATE OR REPLACE SECRET secret (
#                 TYPE s3,
#                 PROVIDER credential_chain,
#                 CHAIN config
#             );
#         """
#         )

#         # PZB-271 just use DuckDB requester pays when this PR gets released: https://github.com/duckdb/duckdb/pull/18258
#         # For now, we need to just download the file temporarily
#         fs = s3fs.S3FileSystem(requester_pays=True)
#         fs.get(
#             f"s3://gfw-data-lake/umd_glad_dist_alerts/parquet/{table}.parquet",
#             f"/tmp/{table}.parquet",
#         )

#         precompute_partial = partial(
#             get_precomputed_statistic_on_gadm_aoi, table=table, intersection=intersection
#         )
#         futures = self.dask_client.map(precompute_partial, aoi["ids"])
#         results = await self.dask_client.gather(futures)
#         alerts_df = pd.concat(results)

#         os.remove(f"/tmp/{table}.parquet")

#         return alerts_df

#     def _execute_on_id(self, query: ParquetQuery, id: str):
#         if query.aoi.type == "admin":
#             # GADM IDs are coming joined by '.', e.g. IDN.24.9
#             admin_id = id.split(".")
#             alerts_df = duckdb.query(query).df()

#             alerts_df["aoi_id"] = id
#             alerts_df["aoi_type"] = "admin"

#             return alerts_df


# class TreeCoverLossParquetHandler():
#     def __init__(self, next_handler, query_service=ParquetQueryService):
#         self.next_handler(self)

#     async def handle(self, analytics_in: TreeCoverLossAnalyticsIn):
#         if analytics_in.aoi.type != "admin":
#             return await self.next_handler(analytics_in)

#         geojsons = await get_geojson(aois)

#         if aois["type"] != "feature_collection":
#             aois = sorted(
#                 [{"type": aois["type"], "id": id} for id in aois["ids"]],
#                 key=lambda aoi: aoi["id"],
#             )
#         else:
#             aois = aois["feature_collection"]["features"]
#             geojsons = [geojson["geometry"] for geojson in geojsons]

#         precompute_partial = partial(zonal_statistics, intersection=intersection)
#         dd_df_futures = await dask_client.gather(
#             dask_client.map(precompute_partial, aois, geojsons)
#         )
#         dfs = await dask_client.gather(dd_df_futures)
#         alerts_df = await dask_client.compute(dd.concat(dfs))

#         return alerts_df


# class TreeCoverLossDaskOnTheFlyHandler():
#     def __init__(self):
#         pass

#     async def handle(self, analytics_command):
#         # run flox against dask cluster
#         pass


# async def get_precomputed_statistics(aoi, intersection, dask_client):
#     if aoi["type"] != "admin" or intersection not in [None, "natural_lands", "driver"]:
#         raise ValueError(
#             f"No precomputed statistics available for AOI type {aoi['type']} and intersection {intersection}"
#         )

#     table = get_precomputed_table(aoi["type"], intersection)

#     # Dumbly doing this per request since the STS token expires eventually otherwise
#     # According to this issue, duckdb should auto refresh the token in 1.3.0,
#     # but it doesn't seem to work for us and people are reporting the same on the issue
#     # https://github.com/duckdb/duckdb-aws/issues/26
#     # TODO do this on lifecycle start once autorefresh works
#     duckdb.query(
#         """
#         CREATE OR REPLACE SECRET secret (
#             TYPE s3,
#             PROVIDER credential_chain,
#             CHAIN config
#         );
#     """
#     )

#     # PZB-271 just use DuckDB requester pays when this PR gets released: https://github.com/duckdb/duckdb/pull/18258
#     # For now, we need to just download the file temporarily
#     fs = s3fs.S3FileSystem(requester_pays=True)
#     fs.get(
#         f"s3://gfw-data-lake/umd_glad_dist_alerts/parquet/{table}.parquet",
#         f"/tmp/{table}.parquet",
#     )

#     precompute_partial = partial(
#         get_precomputed_statistic_on_gadm_aoi, table=table, intersection=intersection
#     )
#     futures = dask_client.map(precompute_partial, aoi["ids"])
#     results = await dask_client.gather(futures)
#     alerts_df = pd.concat(results)

#     os.remove(f"/tmp/{table}.parquet")

#     return alerts_df


# def get_precomputed_table(aoi_type, intersection):
#     if aoi_type == "admin":
#         # Each intersection will be in a different parquet file
#         if intersection is None:
#             table = "gadm_dist_alerts"
#         elif intersection == "driver":
#             table = "gadm_dist_alerts_by_driver"
#         elif intersection == "natural_lands":
#             table = "gadm_dist_alerts_by_natural_lands"
#         else:
#             raise ValueError(f"No way to calculate intersection {intersection}")
#     else:
#         raise ValueError(f"No way to calculate aoi type {aoi_type}")

#     return table


# async def get_precomputed_statistic_on_gadm_aoi(id, table, intersection):
#     # GADM IDs are coming joined by '.', e.g. IDN.24.9
#     gadm_id = id.split(".")

#     query = create_gadm_dist_query(gadm_id, table, intersection)
#     alerts_df = duckdb.query(query).df()

#     alerts_df["aoi_id"] = id
#     alerts_df["aoi_type"] = "admin"

#     return alerts_df
