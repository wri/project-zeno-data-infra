from typing import Callable

import numpy as np
import pandas as pd

AoiIds = Callable[[pd.DataFrame], pd.Series]


def column_ids(column: str) -> AoiIds:
    """Ids taken from one (lowercased) attribute column."""

    def ids(attributes: pd.DataFrame) -> pd.Series:
        return attributes[column].astype(str)

    return ids


def gadm_ids(attributes: pd.DataFrame) -> pd.Series:
    """Each row's GADM id at its own admin level, without GADM's version suffix
    (BRA.1.12_1 -> BRA.1.12), matching the API's admin ids."""
    levels = attributes["adm_level"].astype(int)
    gids = np.select(
        [levels == level for level in (0, 1, 2)],
        [attributes[f"gid_{level}"] for level in (0, 1, 2)],
        default=None,
    )
    return pd.Series(gids, index=attributes.index).str.replace(r"_\d+$", "", regex=True)
