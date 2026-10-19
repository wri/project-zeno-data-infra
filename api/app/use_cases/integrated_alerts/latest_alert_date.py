class LatestAlertDateNotFound(Exception):
    """The admin results have no alert dates to report."""


class IntegratedAlertsLatestDate:
    """Latest date with any alert in the precomputed admin results."""

    def __init__(self, duckdb_query_service):
        self.duckdb_query_service = duckdb_query_service

    async def get(self) -> str:
        data = await self.duckdb_query_service.execute(
            self.build_latest_alert_date_query()
        )
        alert_date = data["alert_date"][0]
        if alert_date is None:
            raise LatestAlertDateNotFound()
        return alert_date

    @staticmethod
    def build_latest_alert_date_query() -> str:
        # The parquet footer already stores the max alert_date of each row group,
        # so reading the metadata avoids scanning the whole column.
        return (
            "SELECT STRFTIME(MAX(CAST(stats_max_value AS DATE)), '%Y-%m-%d') "
            "AS alert_date "
            "FROM parquet_metadata(data_source) "
            "WHERE path_in_schema = 'alert_date'"
        )
