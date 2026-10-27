import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq
from geopandas.io.arrow import _geopandas_to_arrow
from pyproj import Geod

from pipelines.aois.aoi_source import AoiSource


def normalize(features: gpd.GeoDataFrame, source: AoiSource) -> gpd.GeoDataFrame:
    features = (
        features.to_crs("EPSG:4326")
        .rename(columns=str.lower)
        .drop(columns=list(source.dropped_columns))
    )
    return features.assign(aoi_id=source.aoi_ids(features)).reset_index(drop=True)


def drop_missing_ids(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return features[features.aoi_id.notna()]


def changed_aois(
    normalized: gpd.GeoDataFrame,
    identified: gpd.GeoDataFrame,
    repair_report: pd.DataFrame,
) -> pd.DataFrame:
    """One row per feature removed or repaired, with its source attributes so
    features without an id can still be found in the source."""
    missing_ids = normalized.loc[normalized.index.difference(identified.index)]
    invalid = identified.loc[repair_report.index]
    return pd.concat(
        [
            change_rows(missing_ids, "removed", "missing_id", None),
            change_rows(
                invalid,
                repair_report.repaired.map({True: "repaired", False: "removed"}),
                "invalid_geometry",
                repair_report.invalid_reason,
            ),
        ],
        ignore_index=True,
    )


def change_rows(features, change, reason, detail) -> pd.DataFrame:
    attributes = pd.DataFrame(features.drop(columns=["geometry", "aoi_id"]))
    return pd.DataFrame(
        {
            "aoi_id": features.aoi_id,
            "change": change,
            "reason": reason,
            "detail": detail,
            "source_attributes": attributes.apply(lambda row: row.to_json(), axis=1),
        },
        index=features.index,
    )


def with_area_ha(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    geod = Geod(ellps="WGS84")
    return features.assign(
        area_ha=features.geometry.apply(
            lambda geometry: abs(geod.geometry_area_perimeter(geometry)[0]) / 10_000
        )
    )


def write_geoparquet(
    features: gpd.GeoDataFrame, uri: str, row_group_bytes: int = 8 * 2**20
) -> str:
    """Sorted by id in row groups of roughly `row_group_bytes` of geometry, so
    reading a few ids only downloads the row groups whose id range can contain
    them, whether the features are small parcels or whole countries."""
    table = _geopandas_to_arrow(
        features.sort_values("aoi_id"), index=False, write_covering_bbox=True
    )
    sizes = pc.binary_length(table["geometry"]).to_numpy()
    starts = row_group_starts(sizes, row_group_bytes)
    ends = starts[1:] + [len(table)]
    with pq.ParquetWriter(uri, table.schema) as writer:
        for start, end in zip(starts, ends):
            writer.write_table(table.slice(start, end - start))
    return uri


def row_group_starts(sizes: np.ndarray, row_group_bytes: int) -> list[int]:
    """First row of each group, starting a new group each time the running
    total of bytes passes another multiple of `row_group_bytes`."""
    group_numbers = np.cumsum(sizes) // row_group_bytes
    later_starts = np.flatnonzero(np.diff(group_numbers)) + 1
    return [0] + later_starts.tolist()


def write_csv(dataframe: pd.DataFrame, uri: str) -> str:
    dataframe.to_csv(uri, index=False)
    return uri
