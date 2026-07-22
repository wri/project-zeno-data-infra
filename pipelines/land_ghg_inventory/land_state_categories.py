"""AFOLU land_state -> MVP vegetation category mapping (baked in).

The AFOLU flux model encodes each pixel's land-use transition as a ``land_state_node``
code. The model's lookup table maps those 95 codes to broad/detailed land-state
classes; we collapse them into the four MVP vegetation categories:

    tree_loss                      <- detailed_class == "tree_loss"
    tree_gain                      <- detailed_class == "tree_gain"
    trees_remaining_trees          <- detailed_class startswith "tree_tree"
    non_trees_remaining_non_trees  <- broad_class in {"crop", "short_veg"}
    excluded                       <- no_flux (70000000, "Not in decision tree")

Three "mangrove mask" bookkeeping codes share the same
(detailed_class=mangrove_other, broad_class=tree), so ``classify`` can't tell them
apart from their classes; they are assigned per-code from their meanings
(researcher-confirmed):
    10000000  "Before mangrove gain"                 -> tree_gain
    10100000  "Non-mangrove remaining non-mangrove"   -> non_trees_remaining_non_trees
    10200000  "After permanent mangrove loss"         -> tree_loss

The mapping is small (95 codes) and stable, so it is checked in here rather than
fetched at runtime. Source: ``LULUCF_state_node_lookup_table.xlsx`` sheet
``v105_20260601`` (http://gfw2-data.s3.amazonaws.com/climate/AFOLU_flux_model/LULUCF/
state_node_lookup_tables/LULUCF_state_node_lookup_table.xlsx).

To regenerate when the model publishes a new lookup sheet: read that sheet's
``land_state`` / ``land_state_detailed_class`` / ``land_state_broad_class`` columns,
re-apply the collapse rule above, then re-apply the three mangrove-mask overrides.
"""

VEGETATION_CATEGORIES = {
    0: "excluded",
    1: "tree_loss",
    2: "tree_gain",
    3: "trees_remaining_trees",
    4: "non_trees_remaining_non_trees",
}

# land_state codes grouped by category (from sheet v105_20260601)
CATEGORY_LAND_STATES = {
    1: [  # tree_loss
        10200000,  # mangrove mask: "After permanent mangrove loss" (per-code)
        11100000,
        12100000,
        12200000,
        12300000,
        12400000,
        12500000,
        12600000,
        12700000,
        13100000,
        31120000,
        31190000,
        31211200,
        31211900,
        31212200,
        31212900,
        31221200,
        31221900,
        31222200,
        31222900,
        31231200,
        31231900,
        31232200,
        31232900,
        31241200,
        31242200,
        32112000,
        32119000,
        32121200,
        32121900,
        32122200,
        32122900,
        32132000,
        32139000,
        32142000,
        32212000,
        32219000,
        32222000,
        32229000,
        32232000,
        32239000,
        32242000,
        41200000,
        41900000,
    ],
    2: [  # tree_gain
        10000000,  # mangrove mask: "Before mangrove gain" (per-code)
        11200000,
        21100000,
        21200000,
        22100000,
        22200000,
    ],
    3: [  # trees_remaining_trees
        13200000,
        42111120,
        42111190,
        42111220,
        42111290,
        42112120,
        42112190,
        42112220,
        42112290,
        42121120,
        42121190,
        42121220,
        42121290,
        42122120,
        42122190,
        42122220,
        42122290,
        42211200,
        42211900,
        42212200,
        42212900,
        42221120,
        42221190,
        42221212,
        42221219,
        42221222,
        42221229,
        42222200,
        42222900,
    ],
    4: [  # non_trees_remaining_non_trees
        10100000,  # mangrove mask: "Non-mangrove remaining non-mangrove" (per-code)
        51000000,
        52120000,
        52190000,
        52220000,
        52320000,
        52390000,
        53200000,
        53900000,
        61000000,
        62120000,
        62220000,
        62290000,
        63200000,
        63900000,
    ],
    0: [  # excluded ("Not in decision tree", no flux)
        70000000,
    ],
}

# code -> category code; land_state values absent here collapse to 0 (excluded).
LAND_STATE_TO_CATEGORY = {
    code: category for category, codes in CATEGORY_LAND_STATES.items() for code in codes
}


def classify(detailed_class, broad_class) -> int:
    """Collapse a land-state's detailed/broad class into a vegetation category code.

    The base rule from which ``CATEGORY_LAND_STATES`` was generated; it also buckets
    any external table that carries the class columns (e.g. a reference dataset used
    for QC). NOTE: the three mangrove-mask codes (10000000/10100000/10200000) are
    assigned per-code in ``CATEGORY_LAND_STATES`` and are NOT captured here, because
    they share the same (mangrove_other, tree) classes; this returns 0 for them.
    """
    if detailed_class == "tree_loss":
        return 1
    if detailed_class == "tree_gain":
        return 2
    if isinstance(detailed_class, str) and detailed_class.startswith("tree_tree"):
        return 3
    if broad_class in ("crop", "short_veg"):
        return 4
    return 0
