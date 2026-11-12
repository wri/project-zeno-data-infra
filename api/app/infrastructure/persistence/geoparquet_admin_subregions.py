import asyncio
from typing import Any, List, Optional, Tuple

import pyarrow.parquet as parquet

from app.domain.models.aoi_subregion import AoiSubregion
from app.domain.repositories.aoi_subregions import AdminSubregions


class GeoParquetAdminSubregions(AdminSubregions):
    """Reads admin areas from the GADM GeoParquet, which holds every level
    with `adm_level`, the country id `gid_0`, and names per level. An admin
    id holds its parents' ids, so children are found by id prefix."""

    def __init__(self, uri: str, timeout_seconds: float = 60):
        self.uri = uri
        self.timeout_seconds = timeout_seconds

    async def within(self, parent_id: Optional[str], level: int) -> List[AoiSubregion]:
        return await asyncio.wait_for(
            asyncio.to_thread(self._read, parent_id, level),
            timeout=self.timeout_seconds,
        )

    def _read(self, parent_id: Optional[str], level: int) -> List[AoiSubregion]:
        name_column = "country" if level == 0 else f"name_{level}"
        filters: List[Tuple[str, str, Any]] = [("adm_level", "=", level)]
        if parent_id is not None:
            filters.append(("gid_0", "=", parent_id.split(".")[0]))
        table = parquet.read_table(
            self.uri, columns=["aoi_id", "area_ha", name_column], filters=filters
        )

        prefix = f"{parent_id}." if parent_id is not None else ""
        subregions = []
        for row in table.to_pylist():
            if row["aoi_id"].startswith(prefix):
                subregions.append(
                    AoiSubregion(
                        "admin", row["aoi_id"], row[name_column], row["area_ha"]
                    )
                )
        return subregions
