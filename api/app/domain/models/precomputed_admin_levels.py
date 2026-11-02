"""Which admin areas have precomputed results. Deeper admin areas, and every
other area type, are computed on the fly."""

DEEPEST_PRECOMPUTED_ADMIN_LEVEL = 2


def admin_level(admin_id: str) -> int:
    """BRA is level 0, BRA.1 level 1, BRA.1.12 level 2, and so on."""
    return admin_id.count(".")


def has_precomputed_level(admin_id: str) -> bool:
    return admin_level(admin_id) <= DEEPEST_PRECOMPUTED_ADMIN_LEVEL


def has_precomputed_results(aoi) -> bool:
    """Whether every area of the request has precomputed results."""
    return aoi.type == "admin" and all(
        has_precomputed_level(admin_id) for admin_id in aoi.ids
    )
