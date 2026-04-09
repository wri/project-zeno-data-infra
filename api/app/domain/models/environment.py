from enum import StrEnum
from typing import Dict


class Environment(StrEnum):
    test = "test"
    production = "production"
    staging = "staging"


def resolve_uris(
    uri_table: Dict[Environment, Dict[str, str]],
    environment: Environment,
) -> Dict[str, str]:
    """Return production URIs overlaid with environment-specific URIs (if any).

    Production URIs form the base; any specified non-production tier overlays its
    entries (adding new keys or overriding existing ones). The caller gets
    a single flat dict with no missing keys for datasets that only live in
    production.

    Originally it was intended to be able to set up a fallback chain, so for
    example one could say "test" overlaid "staging" which overlaid "production".
    However, at this point I'm not even sure it's desired to have an Environment
    enum instead of arbitrary strings. So for now let's keep it simple and allow
    two levels, an optional overlay and production.
    """
    result: Dict[str, str] = {}

    for env in (Environment.production, environment):
        result.update(uri_table.get(env, {}))

    return result
