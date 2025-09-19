from app.domain.models.dataset import Dataset, DatasetFilter


class TestDatasetFilterStr:
    def test_dataset_filter_str_monotuple(self):
        dsf = DatasetFilter(
            dataset=Dataset.tree_cover_gain,
            op="in",
            value=("2000-2005",),
        )

        assert str(dsf) == "tree_cover_gain_period in ('2000-2005')"

    def test_dataset_filter_str_multituple(self):
        dsf = DatasetFilter(
            dataset=Dataset.tree_cover_gain,
            op="in",
            value=("2000-2005", "2005-2010"),
        )

        assert str(dsf) == "tree_cover_gain_period in ('2000-2005', '2005-2010')"
