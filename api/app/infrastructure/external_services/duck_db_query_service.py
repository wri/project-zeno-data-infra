import asyncio
from concurrent.futures import ProcessPoolExecutor

import duckdb
import pandas as pd

process_pool = ProcessPoolExecutor(max_workers=2)


def run_query_sync(sql: str, params=None):
    con = duckdb.connect(":memory:", config={"threads": "4"})
    try:
        con.execute(
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain,
                CHAIN 'instance;env;config'
            );
        """
        )
        return con.execute(sql, params or []).fetchdf()
    finally:
        con.close()


class DuckDbPrecalcQueryService:
    def __init__(self, table_uri):
        self.table_uri = table_uri

    async def execute(self, query: str) -> pd.DataFrame:
        query = query.replace("data_source", f"'{self.table_uri}'")
        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(process_pool, run_query_sync, query)

        return df.to_dict(orient="list")
