import asyncio
from typing import List

import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import shapely

GADM_AOIS_URI = "s3://lcl-analytics/vectors/aois/gadm/v4.1.85/aois.parquet"


class GadmAoiGeometryRepository:
    """GADM admin area geometries from the AOI table, by aoi_id."""

    def __init__(self, uri: str = GADM_AOIS_URI):
        self.uri = uri

    async def load(self, aoi_type: str, aoi_ids: List[str]):
        """Geometries and areas (ha) of aoi_ids, sorted by aoi_id."""
        if aoi_type != "admin":
            raise ValueError(f"Unable to retrieve AOI type {aoi_type} from GADM.")
        return await asyncio.to_thread(self._load, sorted(set(aoi_ids)))

    def _load(self, aoi_ids: List[str]):
        if self.uri.startswith("s3://"):
            filesystem, path = (
                pafs.S3FileSystem(region="us-east-1"),
                self.uri.removeprefix("s3://"),
            )
        else:
            filesystem, path = pafs.LocalFileSystem(), self.uri
        table = pq.read_table(
            path,
            filesystem=filesystem,
            columns=["aoi_id", "area_ha", "geometry"],
            filters=pc.field("aoi_id").isin(aoi_ids),
        )
        rows = {
            aoi_id: (geometry, area_ha)
            for aoi_id, area_ha, geometry in zip(
                table["aoi_id"].to_pylist(),
                table["area_ha"].to_pylist(),
                table["geometry"].to_pylist(),
            )
        }
        missing = [aoi_id for aoi_id in aoi_ids if aoi_id not in rows]
        if missing:
            raise ValueError(f"GADM areas not found: {', '.join(missing)}")
        geometries = [shapely.from_wkb(rows[aoi_id][0]) for aoi_id in aoi_ids]
        areas_ha = [rows[aoi_id][1] for aoi_id in aoi_ids]
        return geometries, areas_ha
