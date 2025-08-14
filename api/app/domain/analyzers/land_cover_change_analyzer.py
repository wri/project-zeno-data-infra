from abc import abstractmethod

import pandas as pd
import duckdb

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.land_change.land_cover import LandCoverChangeAnalyticsIn
import logging


class LandCoverChangeAnalyzer(Analyzer):
    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository  # LandCoverChangeRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.
        self.results_file = "s3://gfw-data-lake/umd_lcl_land_cover/v2/tabular/statistics/admin_land_cover_change.parquet"

    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = LandCoverChangeAnalyticsIn(**analysis.metadata)
        if land_cover_change_analytics_in.aoi.type == "admin":
            gadm_results = []
            gadm_ids = land_cover_change_analytics_in.aoi.ids
            for gadm_id in gadm_ids:
                results = self._analyze_admin_area(gadm_id)
                results["aoi_id"] = gadm_id
                results["aoi_type"] = "admin"
                results = results[
                    results.land_cover_class_start != results.land_cover_class_end
                ]  # Filter out no change
                gadm_results.append(results)
            combined_results = pd.concat(gadm_results).to_dict(orient="list")
            logging.info(combined_results)
            analyzed_analysis = Analysis(
                combined_results, analysis.metadata, AnalysisStatus.saved
            )
            await self.analysis_repository.store_analysis(
                land_cover_change_analytics_in.thumbprint(), analyzed_analysis
            )

        else:
            raise NotImplementedError()

    def build_query(
        self,
        gadm_id,
    ):
        """
        Build a query to get land cover change statistics for the given GADM IDs.
        """
        from_clause = f"FROM '{self.results_file}'"
        select_clause = "SELECT country"
        where_clause = f"WHERE country = '{gadm_id[0]}'"
        by_clause = "BY country"

        # Includes region, so add relevant filters, selects and group bys
        if len(gadm_id) > 1:
            select_clause += ", region"
            where_clause += f" AND region = {gadm_id[1]}"
            by_clause += ", region"

        # Includes subregion, so add relevant filters, selects and group bys
        if len(gadm_id) > 2:
            select_clause += ", subregion"
            where_clause += f" AND subregion = {gadm_id[2]}"
            by_clause += ", subregion"

        by_clause += ", land_cover_class_start, land_cover_class_end"
        select_clause += ", land_cover_class_start, land_cover_class_end"
        group_by_clause = f"GROUP {by_clause}"
        order_by_clause = f"ORDER {by_clause}"

        # Query and make sure output names match the expected schema (?)
        select_clause += ", SUM(area) AS change_area"
        query = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"

        return query

    def _analyze_admin_area(self, gadm_id):
        duckdb.query(
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain,
                CHAIN config
            );
        """
        )
        gadm_id = gadm_id.split(".")
        query = self.build_query(gadm_id)
        df = duckdb.query(query).df()

        columns_to_drop = ["country"]
        if len(gadm_id) >= 2:
            columns_to_drop += ["region"]
        if len(gadm_id) == 3:
            columns_to_drop += ["subregion"]

        df = df.drop(columns=columns_to_drop, axis=1)

        return df
