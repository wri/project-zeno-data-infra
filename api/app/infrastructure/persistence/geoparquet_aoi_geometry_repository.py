import asyncio

import pyarrow.parquet as parquet
import shapely

from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    AoiNotFoundError,
)


class GeoParquetAoiGeometryRepository(AoiGeometryRepository):
    """Reads AOI geometries from a GeoParquet with `aoi_id`, `area_ha`,
    `geometry` and `bbox` columns."""

    def __init__(self, uri: str, timeout_seconds: float = 60):
        self.uri = uri
        self.timeout_seconds = timeout_seconds

    async def load(self, aoi):
        return await asyncio.wait_for(
            asyncio.to_thread(self._read, aoi.ids), timeout=self.timeout_seconds
        )

    async def bounds(self, aoi):
        return await asyncio.wait_for(
            asyncio.to_thread(self.read_bounds, aoi.ids), timeout=self.timeout_seconds
        )

    def read_bounds(self, aoi_ids):
        table = parquet.read_table(
            self.uri, columns=["aoi_id", "bbox"], filters=[("aoi_id", "in", aoi_ids)]
        )
        boxes = {row["aoi_id"]: row["bbox"] for row in table.to_pylist()}
        missing = [aoi_id for aoi_id in aoi_ids if aoi_id not in boxes]
        if missing:
            raise AoiNotFoundError(f"AOI ids not found: {missing}")
        bounds = []
        for aoi_id in aoi_ids:
            box = boxes[aoi_id]
            bounds.append((box["xmin"], box["ymin"], box["xmax"], box["ymax"]))
        return bounds

    def _read(self, aoi_ids):
        table = parquet.read_table(
            self.uri,
            columns=["aoi_id", "area_ha", "geometry"],
            filters=[("aoi_id", "in", aoi_ids)],
        )
        rows = {row["aoi_id"]: row for row in table.to_pylist()}

        missing = [aoi_id for aoi_id in aoi_ids if aoi_id not in rows]
        if missing:
            raise AoiNotFoundError(f"AOI ids not found: {missing}")

        geometries = [shapely.from_wkb(rows[aoi_id]["geometry"]) for aoi_id in aoi_ids]
        areas_ha = [rows[aoi_id]["area_ha"] for aoi_id in aoi_ids]
        return geometries, areas_ha
