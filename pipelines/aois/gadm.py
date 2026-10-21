import posixpath
import shutil
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

import fsspec
import geopandas as gpd
import pandas as pd

from pipelines.aois.aoi_source import AoiSource


@dataclass(frozen=True)
class GadmSource(AoiSource):
    """GADM admin levels 0-2 from the official by-level distribution, with the
    disputed flag, GIDs and missing names set the same way as the Data API's
    GADM 4.1 import. Source files are cached under `cache_prefix`."""

    source: str
    version: str
    levels_url: str
    all_in_one_url: str
    cache_prefix: str
    name_fills: pd.DataFrame
    dropped_columns: tuple[str, ...] = ()

    def load(self) -> gpd.GeoDataFrame:
        with tempfile.TemporaryDirectory() as directory:
            levels = read_gadm_levels(
                source_geopackage(self.levels_url, self.cache_prefix, directory)
            )
            pieces = read_gadm_pieces(
                source_geopackage(self.all_in_one_url, self.cache_prefix, directory)
            )
            disputed = mark_disputed(levels, pieces)
            return fill_gadm_names(fix_gadm_gids(disputed), self.name_fills)

    def aoi_ids(self, attributes: pd.DataFrame) -> pd.Series:
        return gadm_ids(attributes)


def source_geopackage(url: str, cache_prefix: str, directory: str) -> str:
    """Extracts the GeoPackage from a GADM source zip, downloading it only if it
    isn't cached yet."""
    archive = Path(directory) / Path(url).name
    cached_download(url, f"{cache_prefix}/{archive.name}", archive)
    with zipfile.ZipFile(archive) as zipped:
        geopackage = next(n for n in zipped.namelist() if n.endswith(".gpkg"))
        return zipped.extract(geopackage, directory)


def cached_download(url: str, cache_uri: str, destination: Path) -> None:
    """Copies a file from its cache, or downloads it and fills the cache."""
    cache, cache_path = fsspec.core.url_to_fs(cache_uri)
    if cache.exists(cache_path):
        cache.get_file(cache_path, str(destination))
        return
    download(url, destination)
    cache.makedirs(posixpath.dirname(cache_path), exist_ok=True)
    cache.put_file(str(destination), cache_path)


def download(url: str, destination: Path, timeout_seconds: int = 600) -> None:
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        with open(destination, "wb") as file:
            shutil.copyfileobj(response, file)


def read_gadm_levels(path: str) -> gpd.GeoDataFrame:
    layers = [
        gpd.read_file(path, layer=f"ADM_{level}").assign(ADM_LEVEL=level)
        for level in (0, 1, 2)
    ]
    combined = pd.concat(layers, ignore_index=True)
    return gpd.GeoDataFrame(
        combined.assign(NAME_0=combined.COUNTRY),
        geometry="geometry",
        crs=layers[0].crs,
    )


def read_gadm_pieces(path: str) -> pd.DataFrame:
    """The all-in-one distribution's smallest units, with only the fields the
    disputed flag needs."""
    return gpd.read_file(
        path,
        columns=["GID_0", "GID_1", "GID_2", "DISPUTEDBY"],
        ignore_geometry=True,
    )


def mark_disputed(units: pd.DataFrame, pieces: pd.DataFrame) -> pd.DataFrame:
    """A unit is disputed if it has pieces and every one of them is disputed.
    Pieces are matched on the GIDs the unit has (country, region, subregion),
    as in the Data API's GADM 4.1 import."""
    gid_columns = ["GID_0", "GID_1", "GID_2"]
    pieces = pieces.assign(piece_disputed=has_value(pieces.DISPUTEDBY))
    gid_count = sum(has_value(units[column]).astype(int) for column in gid_columns)

    is_disputed = pd.Series(False, index=units.index)
    for prefix_length in (1, 2, 3):
        prefix = gid_columns[:prefix_length]
        all_pieces_disputed = (
            pieces.astype({column: "string" for column in prefix})
            .groupby(prefix, as_index=False)
            .piece_disputed.all()
        )
        rows = gid_count == prefix_length
        unit_gids = units.loc[rows, prefix].astype("string")
        matched = unit_gids.merge(all_pieces_disputed, on=prefix, how="left")
        is_disputed[rows] = matched.piece_disputed.eq(True).to_numpy()
    return units.assign(is_disputed=is_disputed)


def has_value(column: pd.Series) -> pd.Series:
    return column.fillna("").astype(str).str.strip() != ""


def fix_gadm_gids(features: pd.DataFrame) -> pd.DataFrame:
    """Rewrite GIDs that break the <ISO3>.<region>.<subregion>_<revision>
    pattern, as the Data API's GADM 4.1 import did."""
    ukraine = features.GID_0 == "UKR"
    uk_missing_gid_1 = (
        (features.GID_0 == "GBR") & (features.GID_1 == "NA") & features.GID_2.notna()
    )
    parent_of_gid_2 = features.GID_2.str.rsplit(".", n=1).str[0] + "_1"

    gid_1 = (
        features.GID_1.replace({"CHN.HKG": "CHN.998_1", "CHN.MAC": "CHN.999_1"})
        .mask(ukraine & (features.GID_1 == "?"), "UKR.999_1")
        .mask(uk_missing_gid_1, parent_of_gid_2)
        .str.replace(r"^GHA(?=\d)", "GHA.", regex=True)
    )
    gid_2 = (
        features.GID_2.str.replace(r"^HKG\.", "CHN.998.", regex=True)
        .str.replace(r"^MAC\.", "CHN.999.", regex=True)
        .mask(ukraine & (features.GID_2 == "?"), "UKR.999.999_1")
        .str.replace(r"^GHA(?=\d)", "GHA.", regex=True)
    )
    return features.assign(GID_1=gid_1, GID_2=gid_2)


def fill_gadm_names(features: pd.DataFrame, fills: pd.DataFrame) -> pd.DataFrame:
    """Names GADM leaves as "NA", from a table of (aoi_id, column, name). A fill
    never replaces a name GADM has, and one for an unknown id raises."""
    ids = gadm_ids(
        features[["ADM_LEVEL", "GID_0", "GID_1", "GID_2"]].rename(columns=str.lower)
    )
    filled = features.copy()
    for fill in fills.itertuples():
        rows = ids == fill.aoi_id
        if not rows.any():
            raise ValueError(f"No GADM feature {fill.aoi_id} for name fill {fill.name}")
        filled.loc[rows & (filled[fill.column] == "NA"), fill.column] = fill.name
    return filled


def gadm_ids(attributes: pd.DataFrame) -> pd.Series:
    """Each row's GID at its own admin level, without the revision suffix
    (BRA.1.12_1 -> BRA.1.12) to match the API's admin ids. GADM's placeholder
    GID "NA" can't be requested, so those rows get no id."""
    level = attributes.adm_level.astype(int)
    own_gid = attributes.gid_0.where(
        level == 0, attributes.gid_1.where(level == 1, attributes.gid_2)
    )
    ids = own_gid.str.replace(r"_\d+$", "", regex=True)
    return ids.where(ids != "NA")
