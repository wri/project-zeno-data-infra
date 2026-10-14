import asyncio

import pyarrow.parquet as parquet
import shapely

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository


class GeoParquetAoiGeometryRepository(AoiGeometryRepository):
    """Reads AOI geometries from a GeoParquet file with `aoi_id`, `area_ha`
    and WKB `geometry` columns."""

    def __init__(self, uri: str):
        self.uri = uri

    async def load(self, aoi):
        return await asyncio.to_thread(self._read, aoi.ids)

    def _read(self, aoi_ids):
        table = parquet.read_table(
            self.uri,
            columns=["aoi_id", "area_ha", "geometry"],
            filters=[("aoi_id", "in", aoi_ids)],
        ).to_pydict()
        rows = {
            aoi_id: (geometry, area_ha)
            for aoi_id, area_ha, geometry in zip(
                table["aoi_id"], table["area_ha"], table["geometry"]
            )
        }

        missing = [aoi_id for aoi_id in aoi_ids if aoi_id not in rows]
        if missing:
            raise ValueError(f"AOI ids not found: {missing}")

        geometries = [shapely.from_wkb(rows[aoi_id][0]) for aoi_id in aoi_ids]
        areas_ha = [rows[aoi_id][1] for aoi_id in aoi_ids]
        return geometries, areas_ha
