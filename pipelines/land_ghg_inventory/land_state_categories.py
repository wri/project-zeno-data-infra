"""AFOLU land_state -> MVP vegetation category mapping (baked in).

The AFOLU flux model encodes each pixel's land-use transition as a ``land_state_node``
code. The model's lookup table maps those 95 codes to broad/detailed land-state
classes; we collapse them into the four MVP vegetation categories:

    tree_loss                      <- detailed_class == "tree_loss"
    tree_gain                      <- detailed_class == "tree_gain"
    trees_remaining_trees          <- detailed_class startswith "tree_tree"
    non_trees_remaining_non_trees  <- broad_class in {"crop", "short_veg"}
    excluded                       <- no_flux / mangrove-mask states (no flux)

The mapping is small (95 codes) and stable, so it is checked in here rather than
fetched at runtime. Source: ``LULUCF_state_node_lookup_table.xlsx`` sheet
``v105_20260601`` (http://gfw2-data.s3.amazonaws.com/climate/AFOLU_flux_model/LULUCF/
state_node_lookup_tables/LULUCF_state_node_lookup_table.xlsx).

To regenerate when the model publishes a new lookup sheet: read that sheet's
``land_state`` / ``land_state_detailed_class`` / ``land_state_broad_class`` columns and
re-apply the collapse rule above.
"""

# flake8: noqa: E501  # baked data: long land_state meaning strings

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
    0: [  # excluded (no flux / mangrove-mask bookkeeping states)
        10000000,
        10100000,
        10200000,
        70000000,
    ],
}

# code -> category code; land_state values absent here collapse to 0 (excluded).
LAND_STATE_TO_CATEGORY = {
    code: category for category, codes in CATEGORY_LAND_STATES.items() for code in codes
}

# Full per-code attributes from sheet v105_20260601, for runs that keep the raw
# land_state breakdown (consolidation happens at query time, not before the reduce):
#   code -> (land_state_meaning, broad_class, detailed_class, tall_veg_type)
LAND_STATE_ATTRIBUTES = {
    10000000: (
        "Before mangrove gain (mangrove mask)",
        "tree",
        "mangrove_other",
        "mangrove",
    ),
    10100000: (
        "Non-mangrove remaining non-mangrove (mangrove mask)",
        "tree",
        "mangrove_other",
        "mangrove",
    ),
    10200000: (
        "After permanent mangrove loss (mangrove mask)",
        "tree",
        "mangrove_other",
        "mangrove",
    ),
    11100000: (
        "Gain of mangroves + temp loss in interval",
        "tree",
        "tree_loss",
        "mangrove",
    ),
    11200000: (
        "Gain of mangroves, no loss in interval",
        "tree",
        "tree_gain",
        "mangrove",
    ),
    12100000: ("Temporary loss of mangroves", "tree", "tree_loss", "mangrove"),
    12200000: ("Permanent loss of mangroves to water", "tree", "tree_loss", "mangrove"),
    12300000: (
        "Permanent loss of mangroves to cropland",
        "tree",
        "tree_loss",
        "mangrove",
    ),
    12400000: (
        "Permanent loss of mangroves to settlement",
        "tree",
        "tree_loss",
        "mangrove",
    ),
    12500000: (
        "Permanent loss of mangroves to short vegetation",
        "tree",
        "tree_loss",
        "mangrove",
    ),
    12600000: (
        "Permanent loss of mangroves to tall vegetation",
        "tree",
        "tree_loss",
        "mangrove",
    ),
    12700000: (
        "Permanent loss of mangroves to anything else",
        "tree",
        "tree_loss",
        "mangrove",
    ),
    13100000: (
        "Mangrove remaining mangrove + temp loss in interval",
        "tree",
        "tree_loss",
        "mangrove",
    ),
    13200000: (
        "Mangrove remaining mangrove, no loss in interval",
        "tree",
        "tree_tree_undisturbed",
        "mangrove",
    ),
    21100000: (
        "Gain of oil palm (incl. SDPT oil palm)",
        "tree",
        "tree_gain",
        "oil_palm",
    ),
    21200000: (
        "Gain of non-oil palm planted trees",
        "tree",
        "tree_gain",
        "non_oil_palm_planted_trees",
    ),
    22100000: (
        "Gain of terrestrial natural forest",
        "tree",
        "tree_gain",
        "natural_tree_cover",
    ),
    22200000: (
        "Gain of trees outside forests",
        "tree",
        "tree_gain",
        "trees_in_other_land_covers",
    ),
    31120000: (
        "Full loss of oil palm (incl. SDPT) without fire",
        "tree",
        "tree_loss",
        "oil_palm",
    ),
    31190000: (
        "Full loss of oil palm (incl. SDPT) with fire",
        "tree",
        "tree_loss",
        "oil_palm",
    ),
    31211200: (
        "Full loss of non-oil palm tree crops as cropland without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31211900: (
        "Full loss of non-oil palm tree crops as cropland with fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31212200: (
        "Full loss of non-oil palm planted forest as cropland without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31212900: (
        "Full loss of non-oil palm planted forest as cropland with fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31221200: (
        "Full loss of non-oil palm tree crops as short vegetation without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31221900: (
        "Full loss of non-oil palm tree crops as short vegetation with fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31222200: (
        "Full loss of non-oil palm planted forest as short vegetation without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31222900: (
        "Full loss of non-oil palm planted forest as short vegetation with fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31231200: (
        "Full loss of non-oil palm tree crops to settlement without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31231900: (
        "Full loss of non-oil palm tree crops to settlement with fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31232200: (
        "Full loss of non-oil palm planted forest to settlement without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31232900: (
        "Full loss of non-oil palm planted forest to settlement with fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31241200: (
        "Full loss of non-oil palm tree crops to anything else without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    31242200: (
        "Full loss of non-oil palm planted forest to anything else without fire",
        "tree",
        "tree_loss",
        "non_oil_palm_planted_trees",
    ),
    32112000: (
        "Natural forest converted to cropland without fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32119000: (
        "Natural forest converted to cropland with fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32121200: (
        "Natural forest converted to short vegetation with disturbance that emits all non-soil C pools without fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32121900: (
        "Natural forest converted to short vegetation with disturbance that emits all non-soil C pools with fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32122200: (
        "Natural forest converted to short vegetation with disturbance that emits biomass C pools only without fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32122900: (
        "Natural forest converted to short vegetation with disturbance that emits biomass C pools only with fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32132000: (
        "Natural forest converted to settlement without fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32139000: (
        "Natural forest converted to settlement with fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32142000: (
        "Natural forest converted to anything else (wetland/open water/ice, etc.) without fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    32212000: (
        "Full loss of trees outside forests converted to cropland without fire",
        "tree",
        "tree_loss",
        "trees_in_other_land_covers",
    ),
    32219000: (
        "Full loss of trees outside forests converted to cropland with fire",
        "tree",
        "tree_loss",
        "trees_in_other_land_covers",
    ),
    32222000: (
        "Full loss of trees outside forests converted to short vegetation without fire",
        "tree",
        "tree_loss",
        "trees_in_other_land_covers",
    ),
    32229000: (
        "Full loss of trees outside forests converted to short vegetation with fire",
        "tree",
        "tree_loss",
        "trees_in_other_land_covers",
    ),
    32232000: (
        "Full loss of trees outside forests converted to settlement without fire",
        "tree",
        "tree_loss",
        "trees_in_other_land_covers",
    ),
    32239000: (
        "Full loss of trees outside forests converted to settlement with fire",
        "tree",
        "tree_loss",
        "trees_in_other_land_covers",
    ),
    32242000: (
        "Full loss of trees outside forests converted to anything else without fire",
        "tree",
        "tree_loss",
        "trees_in_other_land_covers",
    ),
    41200000: (
        "Non-planted trees with oil palm planted in the next interval without fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    41900000: (
        "Non-planted trees with oil palm planted in the next interval with fire",
        "tree",
        "tree_loss",
        "natural_tree_cover",
    ),
    42111120: (
        "Oil palm partially disturbed in the current interval with signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "oil_palm",
    ),
    42111190: (
        "Oil palm partially disturbed in the current interval with signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "oil_palm",
    ),
    42111220: (
        "Planted trees partially disturbed in the current interval with signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "non_oil_palm_planted_trees",
    ),
    42111290: (
        "Planted trees partially disturbed in the current interval with signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "non_oil_palm_planted_trees",
    ),
    42112120: (
        "Oil palm partially disturbed in the current interval without signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "oil_palm",
    ),
    42112190: (
        "Oil palm partially disturbed in the current interval without signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "oil_palm",
    ),
    42112220: (
        "Planted trees partially disturbed in the current interval without signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "non_oil_palm_planted_trees",
    ),
    42112290: (
        "Planted trees partially disturbed in the current interval without signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "non_oil_palm_planted_trees",
    ),
    42121120: (
        "Forest partially disturbed in the current interval with signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "natural_tree_cover",
    ),
    42121190: (
        "Forest partially disturbed in the current interval with signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "natural_tree_cover",
    ),
    42121220: (
        "Forest partially disturbed in the current interval without signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "natural_tree_cover",
    ),
    42121290: (
        "Forest partially disturbed in the current interval without signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "natural_tree_cover",
    ),
    42122120: (
        "Trees outside forests partially disturbed in the current interval with signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "trees_in_other_land_covers",
    ),
    42122190: (
        "Trees outside forests partially disturbed in the current interval with signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "trees_in_other_land_covers",
    ),
    42122220: (
        "Trees outside forests partially disturbed in the current interval without signif. height increase after without fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "trees_in_other_land_covers",
    ),
    42122290: (
        "Trees outside forests partially disturbed in the current interval without signif. height increase after with fire",
        "tree",
        "tree_tree_disturbed_height_loss",
        "trees_in_other_land_covers",
    ),
    42211200: (
        "Oil palm not disturbed in the current interval without fire",
        "tree",
        "tree_tree_undisturbed",
        "oil_palm",
    ),
    42211900: (
        "Oil palm not disturbed in the current interval with fire",
        "tree",
        "tree_tree_disturbed_fire_only",
        "oil_palm",
    ),
    42212200: (
        "Planted trees not disturbed in the current interval without fire",
        "tree",
        "tree_tree_undisturbed",
        "non_oil_palm_planted_trees",
    ),
    42212900: (
        "Planted trees not disturbed in the current interval with fire",
        "tree",
        "tree_tree_disturbed_fire_only",
        "non_oil_palm_planted_trees",
    ),
    42221120: (
        "Young secondary natural forest without fire",
        "tree",
        "tree_tree_undisturbed",
        "natural_tree_cover",
    ),
    42221190: (
        "Young secondary natural forest with fire",
        "tree",
        "tree_tree_disturbed_fire_only",
        "natural_tree_cover",
    ),
    42221212: (
        "Primary forest undisturbed since model start without fire",
        "tree",
        "tree_tree_undisturbed",
        "natural_tree_cover",
    ),
    42221219: (
        "Primary forest undisturbed since model start with fire",
        "tree",
        "tree_tree_disturbed_fire_only",
        "natural_tree_cover",
    ),
    42221222: (
        "Old secondary forest undisturbed since model start without fire",
        "tree",
        "tree_tree_undisturbed",
        "natural_tree_cover",
    ),
    42221229: (
        "Old secondary forest undisturbed since model start with fire",
        "tree",
        "tree_tree_disturbed_fire_only",
        "natural_tree_cover",
    ),
    42222200: (
        "Trees outside forests not disturbed in the current interval without fire",
        "tree",
        "tree_tree_undisturbed",
        "trees_in_other_land_covers",
    ),
    42222900: (
        "Trees outside forests not disturbed in the current interval with fire",
        "tree",
        "tree_tree_disturbed_fire_only",
        "trees_in_other_land_covers",
    ),
    51000000: ("Cropland gain", "crop", "crop_gain", "non_tall_vegetation"),
    52120000: (
        "Annual cropland converted to short vegetation without fire",
        "crop",
        "crop_loss",
        "non_tall_vegetation",
    ),
    52190000: (
        "Annual cropland converted to short vegetation with fire",
        "crop",
        "crop_loss",
        "non_tall_vegetation",
    ),
    52220000: (
        "Annual cropland converted to water",
        "crop",
        "crop_loss",
        "non_tall_vegetation",
    ),
    52320000: (
        "Annual cropland converted to anything else without fire",
        "crop",
        "crop_loss",
        "non_tall_vegetation",
    ),
    52390000: (
        "Annual cropland converted to anything else with fire",
        "crop",
        "crop_loss",
        "non_tall_vegetation",
    ),
    53200000: (
        "Cropland remaining cropland without fire",
        "crop",
        "crop_crop_undisturbed",
        "non_tall_vegetation",
    ),
    53900000: (
        "Cropland remaining cropland with fire",
        "crop",
        "crop_crop_undisturbed",
        "non_tall_vegetation",
    ),
    61000000: (
        "Short vegetation gain",
        "short_veg",
        "short_veg_gain",
        "non_tall_vegetation",
    ),
    62120000: (
        "Short vegetation loss converted to water",
        "short_veg",
        "short_veg_loss",
        "non_tall_vegetation",
    ),
    62220000: (
        "Short vegetation loss converted to non-water without fire",
        "short_veg",
        "short_veg_loss",
        "non_tall_vegetation",
    ),
    62290000: (
        "Short vegetation loss converted to non-water with fire",
        "short_veg",
        "short_veg_loss",
        "non_tall_vegetation",
    ),
    63200000: (
        "Short vegetation remaining short vegetation without fire",
        "short_veg",
        "short_veg_short_veg_undisturbed",
        "non_tall_vegetation",
    ),
    63900000: (
        "Short vegetation remaining short vegetation with fire",
        "short_veg",
        "short_veg_short_veg_undisturbed",
        "non_tall_vegetation",
    ),
    70000000: ("Not in decision tree", "no_flux", "no_flux", "non_tall_vegetation"),
}

# sorted land_state codes, e.g. for flox expected_groups on the land_state axis.
LAND_STATE_CODES = tuple(sorted(LAND_STATE_ATTRIBUTES))


def classify(detailed_class, broad_class) -> int:
    """Collapse a land-state's detailed/broad class into a vegetation category code.

    This is the canonical rule from which ``CATEGORY_LAND_STATES`` was generated; it
    also lets us bucket any external table that carries the class columns (e.g. a
    reference dataset used for QC).
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
