"""Maps the AFOLU flux model's 95 ``land_state_node`` codes to vegetation categories.

Each pixel's land-use transition is one of 95 codes; they collapse to four categories:

    tree_loss                      <- detailed_class == "tree_loss"
    tree_gain                      <- detailed_class == "tree_gain"
    trees_remaining_trees          <- detailed_class startswith "tree_tree"
    non_trees_remaining_non_trees  <- broad_class in {"crop", "short_veg"}
    excluded                       <- no_flux (70000000)

The three mangrove-mask codes (10000000/10100000/10200000) share one
detailed/broad class, so they are mapped by code from their transition meaning:
"Before mangrove gain" -> tree_gain, "Non-mangrove remaining non-mangrove" ->
non_trees_remaining_non_trees, "After permanent mangrove loss" -> tree_loss.

Baked in rather than fetched at runtime. Source: ``LULUCF_state_node_lookup_table.xlsx``
sheet ``v105_20260601`` (http://gfw2-data.s3.amazonaws.com/climate/AFOLU_flux_model/
LULUCF/state_node_lookup_tables/LULUCF_state_node_lookup_table.xlsx).
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
        10200000,  # mangrove mask: after permanent mangrove loss
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
        10000000,  # mangrove mask: before mangrove gain
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
        10100000,  # mangrove mask: non-mangrove remaining non-mangrove
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
    """Map a land-state's detailed/broad class to a vegetation category code.

    Buckets any table carrying the class columns (e.g. a reference dataset). This
    is the class-based rule only; the mangrove-mask codes are mapped by code in
    ``CATEGORY_LAND_STATES`` and return 0 here.
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
