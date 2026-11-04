import pyarrow as pa
import pyarrow.parquet as pq


def read_aoi_attributes(uri: str) -> pa.Table:
    """Every column of an AOI GeoParquet except its geometry and bbox."""
    columns = [
        name for name in pq.read_schema(uri).names if name not in ("geometry", "bbox")
    ]
    return pq.read_table(uri, columns=columns)
