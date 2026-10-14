from typing import Callable

import pandas as pd

AoiIds = Callable[[pd.DataFrame], pd.Series]


def column_ids(column: str) -> AoiIds:
    """Ids taken from one (lowercased) attribute column."""

    def ids(attributes: pd.DataFrame) -> pd.Series:
        return attributes[column].astype(str)

    return ids
