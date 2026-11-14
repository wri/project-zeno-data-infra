import asyncio
from typing import List

import pyarrow.compute as compute
import pyarrow.dataset as dataset
import shapely
from shapely.geometry.base import BaseGeometry

from app.domain.models.aoi_subregion import AoiSubregion
from app.domain.repositories.aoi_subregions import IntersectingSubregions


class GeoParquetIntersectingSubregions(IntersectingSubregions):
    """Reads one type of AOI from a GeoParquet with `aoi_id`, `area_ha`, a
    name column, `geometry` and a `bbox` covering column. Rows are pruned by
    bounding box first; an AOI counts as inside when a point within it lies
    inside the parent, so neighbors that only share a border are left out."""

    def __init__(
        self,
        uri: str,
        aoi_type: str,
        name_column: str,
        timeout_seconds: float = 60,
    ):
        self.uri = uri
        self.aoi_type = aoi_type
        self.name_column = name_column
        self.timeout_seconds = timeout_seconds

    async def within(self, geometry: BaseGeometry) -> List[AoiSubregion]:
        return await asyncio.wait_for(
            asyncio.to_thread(self._read, geometry), timeout=self.timeout_seconds
        )

    def _read(self, geometry: BaseGeometry) -> List[AoiSubregion]:
        xmin, ymin, xmax, ymax = geometry.bounds
        overlapping = (
            (compute.field("bbox", "xmin") <= xmax)
            & (compute.field("bbox", "xmax") >= xmin)
            & (compute.field("bbox", "ymin") <= ymax)
            & (compute.field("bbox", "ymax") >= ymin)
        )
        table = dataset.dataset(self.uri, format="parquet").to_table(
            columns=["aoi_id", "area_ha", self.name_column, "geometry"],
            filter=overlapping,
        )

        shapely.prepare(geometry)
        subregions = []
        for row in table.to_pylist():
            point = shapely.from_wkb(row["geometry"]).representative_point()
            if geometry.contains(point):
                subregions.append(
                    AoiSubregion(
                        self.aoi_type,
                        row["aoi_id"],
                        row[self.name_column],
                        row["area_ha"],
                    )
                )
        return subregions
