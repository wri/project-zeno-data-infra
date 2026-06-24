import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Dict

import duckdb

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
    def __init__(self, table_uri, timeout_seconds: float = 300):
        self.table_uri = table_uri
        self.timeout_seconds = timeout_seconds

    async def execute(self, query: str) -> Dict:
        # replace data_source in query FROM with actual table URI
        query = query.replace("data_source", f"'{self.table_uri}'")
        df = await asyncio.wait_for(self._run(query), timeout=self.timeout_seconds)
        return df.to_dict(orient="list")

    async def _run(self, query: str):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(process_pool, run_query_sync, query)
