from typing import List

from shapely import Geometry


class DataApiAoiGeometryRepository:
    def load(self, aoi_type: str, aoi_ids: List[str]) -> Geometry:
        pass
