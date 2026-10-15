import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable

import geopandas as gpd
import numpy as np
import pandas as pd


def gadm_levels(url: str, levels=(0, 1, 2)) -> Callable[[], gpd.GeoDataFrame]:
    """Loads the GADM distribution that keeps each admin level in its own layer,
    with GIDs fixed the same way as the Data API's GADM 4.1 import."""

    def load() -> gpd.GeoDataFrame:
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "gadm_levels.zip"
            download(url, archive)
            with zipfile.ZipFile(archive) as zipped:
                geopackage = next(
                    name for name in zipped.namelist() if name.endswith(".gpkg")
                )
                zipped.extract(geopackage, directory)
            features = read_gadm_levels(str(Path(directory) / geopackage), levels)
        return fix_gadm_gids(features)

    return load


def download(url: str, destination: Path, timeout_seconds: int = 600) -> None:
    """Stream a file to disk, failing if the server stops responding."""
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        with open(destination, "wb") as file:
            shutil.copyfileobj(response, file)


def read_gadm_levels(path: str, levels=(0, 1, 2)) -> gpd.GeoDataFrame:
    """Combine the ADM_n layers into one, recording each row's level and
    naming countries in NAME_0 to match NAME_1 and NAME_2."""
    layers = [
        gpd.read_file(path, layer=f"ADM_{level}").assign(ADM_LEVEL=level)
        for level in levels
    ]
    combined = pd.concat(layers, ignore_index=True)
    return gpd.GeoDataFrame(
        combined.assign(NAME_0=combined["COUNTRY"]),
        geometry="geometry",
        crs=layers[0].crs,
    )


def fix_gadm_gids(features: pd.DataFrame) -> pd.DataFrame:
    """GADM 4.1 GIDs that break the <ISO3>.<region>.<subregion>_<revision>
    pattern, rewritten the same way as the Data API's GADM 4.1 import: Hong
    Kong and Macau become Chinese regions 998 and 999, Ukraine's unnamed
    region becomes 999, UK rows missing a GID_1 take it from GID_2, and Ghana
    GIDs get their missing separator."""
    fixed = features.copy()
    country = fixed["GID_0"]
    level = fixed["ADM_LEVEL"].astype(int)
    gid_1 = fixed["GID_1"].astype("string")
    gid_2 = fixed["GID_2"].astype("string")

    new_gid_1 = gid_1.copy()
    new_gid_1[gid_1 == "CHN.HKG"] = "CHN.998_1"
    new_gid_1[gid_1 == "CHN.MAC"] = "CHN.999_1"
    new_gid_1[(country == "UKR") & (gid_1 == "?")] = "UKR.999_1"
    uk_missing = (country == "GBR") & (gid_1 == "NA") & gid_2.notna()
    new_gid_1[uk_missing] = gid_2[uk_missing].str.rsplit(".", n=1).str[0] + "_1"

    new_gid_2 = gid_2.copy()
    for prefix, region in (("HKG", "998"), ("MAC", "999")):
        territory = gid_2.str.startswith(prefix, na=False)
        new_gid_2[territory] = (
            f"CHN.{region}." + gid_2[territory].str.split(".", n=1).str[1]
        )
    new_gid_2[(country == "UKR") & (gid_2 == "?")] = "UKR.999.999_1"

    ghana = country == "GHA"
    ghana_gid_1 = ghana & (level >= 1) & ~new_gid_1.str.contains(".", regex=False)
    new_gid_1[ghana_gid_1] = new_gid_1[ghana_gid_1].str.replace(
        r"^(...)", r"\1.", regex=True
    )
    ghana_gid_2 = ghana & (level == 2) & (new_gid_2.str.count(r"\.") < 2)
    new_gid_2[ghana_gid_2] = new_gid_2[ghana_gid_2].str.replace(
        r"^(...)", r"\1.", regex=True
    )

    fixed["GID_1"] = new_gid_1.astype(object).where(new_gid_1.notna(), None)
    fixed["GID_2"] = new_gid_2.astype(object).where(new_gid_2.notna(), None)
    return fixed


def gadm_ids(attributes: pd.DataFrame) -> pd.Series:
    """Each row's GADM id at its own admin level, without GADM's revision
    suffix (BRA.1.12_1 -> BRA.1.12), matching the API's admin ids. GADM's
    placeholder id NA can't be requested, so those rows get no id."""
    levels = attributes["adm_level"].astype(int)
    gids = np.select(
        [levels == level for level in (0, 1, 2)],
        [
            attributes.get(f"gid_{level}", pd.Series(None, index=attributes.index))
            for level in (0, 1, 2)
        ],
        default=None,
    )
    ids = pd.Series(gids, index=attributes.index, dtype="object").str.replace(
        r"_\d+$", "", regex=True
    )
    return ids.where(ids != "NA")
