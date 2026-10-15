import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable

import geopandas as gpd
import pandas as pd


def gadm_levels(url: str) -> Callable[[], gpd.GeoDataFrame]:
    """GADM admin levels 0-2 from the official by-level distribution, with GIDs
    fixed the same way as the Data API's GADM 4.1 import."""

    def load() -> gpd.GeoDataFrame:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "gadm_levels.zip"
            download(url, archive)
            with zipfile.ZipFile(archive) as zipped:
                geopackage = next(n for n in zipped.namelist() if n.endswith(".gpkg"))
                path = zipped.extract(geopackage, directory)
            return fix_gadm_gids(read_gadm_levels(path))

    return load


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
