import asyncio
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TypeVar

import pandas as pd
from pydantic import BaseModel

from app.domain.models.precomputed_admin_levels import PrecomputedAdminLevels

Request = TypeVar("Request", bound=BaseModel)
Results = Dict[str, Any]


async def analyze_by_precomputed_levels(
    request: Request,
    levels: PrecomputedAdminLevels,
    precomputed: Callable[[Request], Awaitable[Results]],
    on_the_fly: Callable[[Request], Awaitable[Results]],
) -> Results:
    """Analyzes the request's areas from precomputed results where they exist
    and on the fly otherwise. Admin requests that mix both get two analyses,
    run at the same time, and one combined result."""
    precomputed_request, on_the_fly_request = split_by_precomputed_levels(
        request, levels
    )
    analyses = []
    if precomputed_request is not None:
        analyses.append(precomputed(precomputed_request))
    if on_the_fly_request is not None:
        analyses.append(on_the_fly(on_the_fly_request))
    return combined_rows(await asyncio.gather(*analyses))


def split_by_precomputed_levels(
    request: Request, levels: PrecomputedAdminLevels
) -> Tuple[Optional[Request], Optional[Request]]:
    """The request for areas with precomputed results and the request for the
    rest, either of which may be None."""
    aoi = request.aoi  # type: ignore[attr-defined]
    if levels.covers(aoi):
        return request, None
    precomputed_ids = []
    if aoi.type == "admin":
        precomputed_ids = [
            admin_id for admin_id in aoi.ids if levels.includes(admin_id)
        ]
    if not precomputed_ids:
        return None, request
    deeper_ids = [admin_id for admin_id in aoi.ids if not levels.includes(admin_id)]
    return with_ids(request, precomputed_ids), with_ids(request, deeper_ids)


def with_ids(request: Request, ids: list[str]) -> Request:
    aoi = request.aoi.model_copy(update={"ids": ids})  # type: ignore[attr-defined]
    return request.model_copy(update={"aoi": aoi})


def combined_rows(results: list[Results]) -> Results:
    """The rows of every column-oriented result. A column given as a single
    value applies to all of its result's rows."""
    if len(results) == 1:
        return results[0]
    columns = [set(result) for result in results]
    all_columns = set().union(*columns)
    if any(result_columns != all_columns for result_columns in columns):
        shared_columns = all_columns.intersection(*columns)
        raise ValueError(
            "Precomputed and on-the-fly results have different columns: "
            f"{sorted(all_columns - shared_columns)}"
        )
    frames = [pd.DataFrame(result) for result in results]
    return pd.concat(frames, ignore_index=True).to_dict(orient="list")
