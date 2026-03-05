import pytest

from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
)

SAMPLE_FEATURE_COLLECTION = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "poly_1",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [10.0, 0.0],
                        [10.0, 1.0],
                        [11.0, 1.0],
                        [11.0, 0.0],
                        [10.0, 0.0],
                    ]
                ],
            },
        }
    ],
}


class TestAdminAreaOfInterest:
    def test_aaoi_happy_path(self):
        assert AdminAreaOfInterest(type="admin", ids=["ETH"])

    def test_aaoi_regex_consecutive_periods(self):
        with pytest.raises(ValueError):
            _ = AdminAreaOfInterest(type="admin", ids=["ETH.1..2"])

    def test_aaoi_max_level_is_2(self):
        with pytest.raises(ValueError):
            _ = AdminAreaOfInterest(type="admin", ids=["ETH.1.2.3"])

    def test_aaoi_regex_no_empty_strings(self):
        with pytest.raises(ValueError):
            _ = AdminAreaOfInterest(type="admin", ids=[""])

    def test_aaoi_regex_isos_must_have_len_3(self):
        with pytest.raises(ValueError):
            _ = AdminAreaOfInterest(type="admin", ids=["US"])

    def test_aaoi_regex_sub_admins_must_be_stringified_ints(self):
        with pytest.raises(ValueError):
            _ = AdminAreaOfInterest(type="admin", ids=["USA.1.lizard"])


class TestCustomAreaOfInterestHash:
    def test_compute_geometry_hash_is_deterministic(self):
        aoi = CustomAreaOfInterest(
            feature_collection=SAMPLE_FEATURE_COLLECTION,
        )
        assert aoi.compute_geometry_hash() == aoi.compute_geometry_hash()

    def test_same_geometry_produces_same_hash(self):
        aoi1 = CustomAreaOfInterest(
            feature_collection=SAMPLE_FEATURE_COLLECTION,
        )
        aoi2 = CustomAreaOfInterest(
            feature_collection=SAMPLE_FEATURE_COLLECTION,
        )
        assert aoi1.compute_geometry_hash() == aoi2.compute_geometry_hash()

    def test_different_geometry_produces_different_hash(self):
        aoi1 = CustomAreaOfInterest(
            feature_collection=SAMPLE_FEATURE_COLLECTION,
        )
        other_fc = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "poly_2",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [20.0, 0.0],
                                [20.0, 1.0],
                                [21.0, 1.0],
                                [21.0, 0.0],
                                [20.0, 0.0],
                            ]
                        ],
                    },
                }
            ],
        }
        aoi2 = CustomAreaOfInterest(feature_collection=other_fc)
        assert aoi1.compute_geometry_hash() != aoi2.compute_geometry_hash()

    def test_hash_is_sha256_hex(self):
        aoi = CustomAreaOfInterest(
            feature_collection=SAMPLE_FEATURE_COLLECTION,
        )
        h = aoi.compute_geometry_hash()
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)
