# flake8: noqa: E501
import json
from enum import Enum
from typing import Iterable

import duckdb
import xarray as xr
from rioxarray.exceptions import NoDataInBounds
from shapely.geometry import mapping, shape

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository

JULIAN_DATE_2021 = 2459215


class FeatureTooSmallError(Exception):
    """Raised when an AOI is too small to contain any pixel centroids."""

    pass


async def get_geojson(aoi, aoi_geometry_repository: AoiGeometryRepository | None):
    """GeoJSON geometries for an AOI. A predefined AOI's geometries come in the
    order of its ids sorted as strings, which is how callers pair them up."""
    if aoi.type == "feature_collection":
        return aoi.feature_collection["features"]
    if aoi_geometry_repository is None:
        raise ValueError(f"No geometry source for AOI type {aoi.type}.")
    sorted_aoi = aoi.model_copy(update={"ids": sorted(aoi.ids)})
    geometries, _ = await aoi_geometry_repository.load(sorted_aoi)
    return [mapping(geometry) for geometry in geometries]


def clip_zarr_to_geojson(xarr: xr.Dataset, geojson):
    geom = shape(geojson)

    sliced: xr.Dataset = xarr.sel(
        x=slice(geom.bounds[0], geom.bounds[2]),
        y=slice(geom.bounds[3], geom.bounds[1]),
    )
    if "band" in sliced.dims:
        sliced = sliced.squeeze("band")

    # Exit early if the geometry is fully out of bounds of the dataset, so all the
    # data variables are already empty. Will take Justin's better fix.
    if all(d.size == 0 for d in sliced.data_vars.values()):
        return sliced

    try:
        clipped = sliced.rio.clip([geojson])
    except NoDataInBounds:
        raise FeatureTooSmallError("AOI is too small. Please select a larger AOI ")
    return clipped


def read_zarr_clipped_to_geojson(uri, geojson, group: str | None = None):
    zarr = read_zarr(uri, group=group)
    if not zarr.dims:
        raise ValueError(
            f"Zarr at {uri} (group={group!r}) opened with no dimensions. "
            "The zarr may be a group container requiring a 'group' parameter, "
            "or the store structure may have changed."
        )
    zarr.rio.write_crs("EPSG:4326", inplace=True)
    clipped = clip_zarr_to_geojson(zarr, geojson)
    return clipped


def read_zarr(uri, group: str | None = None):
    return _open_zarr(uri, group=group)


def _open_zarr(uri, group: str | None = None):
    return xr.open_zarr(
        uri,
        group=group,
        storage_options={"requester_pays": True},
    )


def get_sql_in_list(iter: Iterable) -> str:
    quoted = [f"'{item}'" for item in iter]
    joined = f"({', '.join(quoted)})"
    return joined


def initialize_duckdb():
    # Dumbly doing this per request since the STS token expires eventually otherwise
    # According to this issue, duckdb should auto refresh the token in 1.3.0,
    # but it doesn't seem to work for us and people are reporting the same on the issue
    # https://github.com/duckdb/duckdb-aws/issues/26
    # TODO do this on lifecycle start once autorefresh works
    duckdb.query(
        """
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER credential_chain,
            CHAIN 'instance;env;config'
        );
    """
    )


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)
