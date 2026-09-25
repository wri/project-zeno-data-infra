import geopandas as gpd
from pipelines.concessions.geometry_repair import repair_geometries
from shapely import Polygon, box

BOWTIE = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])
SPIKE = Polygon([(0, 0), (2, 0), (2, 1), (3, 1), (2, 1), (2, 2), (0, 2), (0, 0)])
COLLINEAR = Polygon([(0, 0), (1, 1), (2, 2), (0, 0)])


def features(*geometries):
    return gpd.GeoDataFrame(
        {"aoi_id": [str(i + 1) for i in range(len(geometries))]},
        geometry=list(geometries),
        crs="EPSG:4326",
    )


def test_self_intersecting_polygon_is_made_valid():
    repaired, report = repair_geometries(features(box(0, 0, 1, 1), BOWTIE))

    assert repaired.is_valid.all()
    assert list(repaired.aoi_id) == ["1", "2"]
    assert report.to_dict("records") == [
        {
            "aoi_id": "2",
            "invalid_reason": "Self-intersection[1 1]",
            "repaired": True,
        }
    ]


def test_non_polygon_parts_are_dropped_from_repaired_geometry():
    repaired, _ = repair_geometries(features(SPIKE))

    assert repaired.geometry.iloc[0].geom_type == "Polygon"
    assert repaired.geometry.iloc[0].area == 4.0


def test_unrepairable_geometry_is_reported_and_dropped():
    repaired, report = repair_geometries(features(box(0, 0, 1, 1), COLLINEAR))

    assert list(repaired.aoi_id) == ["1"]
    assert report.loc[0, "aoi_id"] == "2"
    assert not report.loc[0, "repaired"]


def test_valid_input_produces_empty_report():
    repaired, report = repair_geometries(features(box(0, 0, 1, 1)))

    assert len(repaired) == 1
    assert report.empty
