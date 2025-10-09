import pytest

from app.models.common.areas_of_interest import AdminAreaOfInterest


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
