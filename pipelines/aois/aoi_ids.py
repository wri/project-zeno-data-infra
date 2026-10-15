from typing import Callable

import pandas as pd

AoiIds = Callable[[pd.DataFrame], pd.Series]


def column_ids(column: str) -> AoiIds:
    """Ids taken from one (lowercased) attribute column."""

    def ids(attributes: pd.DataFrame) -> pd.Series:
        values = attributes[column]
        if pd.api.types.is_float_dtype(values):
            # integer ids read as floats when the column has gaps (101 -> 101.0)
            values = values.astype("Int64")
        return values.astype("string")

    return ids
