import pandas as pd
import pyarrow.parquet as pq


def read_aoi_attributes(uri: str) -> pd.DataFrame:
    """Every column of an AOI GeoParquet except its geometry and bbox."""
    columns = [
        name for name in pq.read_schema(uri).names if name not in ("geometry", "bbox")
    ]
    return pq.read_table(uri, columns=columns).to_pandas()
