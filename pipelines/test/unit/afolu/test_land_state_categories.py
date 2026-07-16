from collections import Counter

from pipelines.afolu.land_state_categories import (
    LAND_STATE_TO_CATEGORY,
    VEGETATION_CATEGORIES,
)


def test_mapping_covers_all_95_land_states():
    assert len(LAND_STATE_TO_CATEGORY) == 95


def test_per_category_counts_match_lookup():
    # from sheet v105_20260601
    assert Counter(LAND_STATE_TO_CATEGORY.values()) == {1: 43, 2: 5, 3: 29, 4: 14, 0: 4}


def test_representative_codes_map_to_expected_category():
    assert LAND_STATE_TO_CATEGORY[11100000] == 1  # tree_loss
    assert LAND_STATE_TO_CATEGORY[21100000] == 2  # tree_gain
    assert LAND_STATE_TO_CATEGORY[13200000] == 3  # trees_remaining_trees
    assert LAND_STATE_TO_CATEGORY[51000000] == 4  # non_trees_remaining_non_trees
    assert LAND_STATE_TO_CATEGORY[70000000] == 0  # excluded (no flux)
    assert VEGETATION_CATEGORIES[3] == "trees_remaining_trees"
