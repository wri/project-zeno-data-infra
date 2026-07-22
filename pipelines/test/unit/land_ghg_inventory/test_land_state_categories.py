from collections import Counter

from pipelines.land_ghg_inventory.land_state_categories import (
    LAND_STATE_TO_CATEGORY,
    VEGETATION_CATEGORIES,
)


def test_mapping_covers_all_95_land_states():
    assert len(LAND_STATE_TO_CATEGORY) == 95


def test_per_category_counts_match_lookup():
    # from sheet v105_20260601, with the 3 mangrove-mask codes assigned per-code
    # (research-confirmed): 10200000->tree_loss, 10000000->tree_gain,
    # 10100000->non_trees; only 70000000 (Not in decision tree) stays excluded.
    assert Counter(LAND_STATE_TO_CATEGORY.values()) == {1: 44, 2: 6, 3: 29, 4: 15, 0: 1}


def test_representative_codes_map_to_expected_category():
    assert LAND_STATE_TO_CATEGORY[11100000] == 1  # tree_loss
    assert LAND_STATE_TO_CATEGORY[21100000] == 2  # tree_gain
    assert LAND_STATE_TO_CATEGORY[13200000] == 3  # trees_remaining_trees
    assert LAND_STATE_TO_CATEGORY[51000000] == 4  # non_trees_remaining_non_trees
    assert LAND_STATE_TO_CATEGORY[70000000] == 0  # excluded (Not in decision tree)
    assert VEGETATION_CATEGORIES[3] == "trees_remaining_trees"


def test_mangrove_mask_codes_assigned_per_code():
    # same (detailed=mangrove_other, broad=tree) for all three, so classify() can't
    # split them; assigned by code from the researcher-confirmed meanings.
    assert LAND_STATE_TO_CATEGORY[10000000] == 2  # "Before mangrove gain" -> tree_gain
    assert LAND_STATE_TO_CATEGORY[10100000] == 4  # non-mangrove remaining -> non_trees
    assert LAND_STATE_TO_CATEGORY[10200000] == 1  # after mangrove loss -> tree_loss
