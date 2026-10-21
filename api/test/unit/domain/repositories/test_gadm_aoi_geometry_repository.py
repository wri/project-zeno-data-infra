import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from app.domain.repositories.gadm_aoi_geometry_repository import (
    GadmAoiGeometryRepository,
)


@pytest.fixture
def aois_uri(tmp_path):
    geometries = {
        "BRA": shapely.box(-74, -34, -34, 5),
        "BRA.14": shapely.box(-59, -10, -46, 3),
        "BRA.14.87": shapely.box(-53, -4, -52, -3),
    }
    table = pa.table(
        {
            "aoi_id": list(geometries),
            "adm_level": [0, 1, 2],
            "area_ha": [851.0, 124.0, 1.0],
            "geometry": [shapely.to_wkb(g) for g in geometries.values()],
        }
    )
    path = tmp_path / "aois.parquet"
    pq.write_table(table, path)
    return str(path), geometries


@pytest.mark.asyncio
async def test_loads_geometries_sorted_by_aoi_id(aois_uri):
    uri, geometries = aois_uri
    loaded, areas_ha = await GadmAoiGeometryRepository(uri).load(
        "admin", ["BRA.14.87", "BRA"]
    )
    assert [g.equals(geometries[i]) for g, i in zip(loaded, ["BRA", "BRA.14.87"])] == [
        True,
        True,
    ]
    assert areas_ha == [851.0, 1.0]


@pytest.mark.asyncio
async def test_missing_aoi_raises(aois_uri):
    uri, _ = aois_uri
    with pytest.raises(ValueError, match="GADM areas not found: BRA.99"):
        await GadmAoiGeometryRepository(uri).load("admin", ["BRA", "BRA.99"])


@pytest.mark.asyncio
async def test_only_admin_aois(aois_uri):
    uri, _ = aois_uri
    with pytest.raises(ValueError):
        await GadmAoiGeometryRepository(uri).load("protected_area", ["1"])
