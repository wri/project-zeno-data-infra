from pathlib import Path

import pandas as pd

from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.gadm import GadmSource
from pipelines.aois.vector_file_source import VectorFileSource


def aoi_sources() -> tuple[AoiSource, ...]:
    """Every AOI dataset the pipeline builds, at its pinned version."""
    gadm_downloads = "https://geodata.ucdavis.edu/gadm/gadm4.1"
    return (
        GadmSource(
            source="gadm",
            version="v4.1.85",
            levels_url=f"{gadm_downloads}/gadm_410-levels.zip",
            all_in_one_url=f"{gadm_downloads}/gadm_410-gpkg.zip",
            cache_prefix="s3://lcl-analytics/vectors/aois/gadm/v4.1.85/source",
            name_fills=pd.read_csv(Path(__file__).parent / "gadm_name_fills.csv"),
        ),
        VectorFileSource(
            source="wdpa",
            version="v202512",
            uri="s3://gfw-data-lake/wdpa_protected_areas/v202512/raw/"
            "WDPA_Dec2025_Public.gdb.zip",
            layer="WDPA_poly_Dec2025",
            id_column="site_pid",
        ),
        VectorFileSource(
            source="kba",
            version="v20250911",
            uri="s3://gfw-data-lake/birdlife_key_biodiversity_areas/v20240904/raw/"
            "KBAsGlobal_2024_September_03_POL.zip",
            id_column="sitrecid",
            # Esri-computed in the source projection, so stale after reprojecting
            dropped_columns=("shape_leng", "shape_area"),
        ),
        VectorFileSource(
            source="landmark",
            version="v20260617",
            uri="s3://wri-lcl-public-unsafe/20260617_IP_LC_and_Indicative.gdb.zip",
            layer="IP_LC_Indicative_20260617",
            id_column="landmark_id",
            # Esri-computed in the source projection, so stale after reprojecting
            dropped_columns=("shape_length", "shape_area"),
        ),
    )


def select_aoi_sources(
    sources: tuple[AoiSource, ...], name: str | None
) -> tuple[AoiSource, ...]:
    if name is None:
        return sources
    by_name = {source.source: source for source in sources}
    if name not in by_name:
        raise ValueError(
            f"Unknown AOI source '{name}'. Accepted values: {', '.join(by_name)}"
        )
    return (by_name[name],)
