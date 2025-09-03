import duckdb
import pandas as pd


class DuckDbPrecalcQueryService:
    def __init__(self, table_uri):
        self.table_uri = table_uri

    async def execute(self, query: str) -> pd.DataFrame:
        self.initialize_duckdb()

        # need to declare this to bind FROM in SQL query
        data_source = duckdb.read_parquet(self.table_uri)  # noqa: F841

        # TODO duckdb has no native async, need to use aioduckdb? Check if blocking in load test
        df = duckdb.sql(query).df()
        return df.to_dict(orient="list")

    def initialize_duckdb(self):
        # Dumbly doing this per request since the STS token expires eventually otherwise
        # According to this issue, duckdb should auto refresh the token in 1.3.0,
        # but it doesn't seem to work for us and people are reporting the same on the issue
        # https://github.com/duckdb/duckdb-aws/issues/26
        # TODO do this on lifecycle start once autorefresh works
        duckdb.query(
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain,
                CHAIN 'instance;env;config'
            );
        """
        )
