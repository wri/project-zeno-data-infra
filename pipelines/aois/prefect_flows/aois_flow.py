from datetime import date

from pipelines.aois import stages
from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.geometry_repair import repair_geometries
from pipelines.aois.prefect_flows.aoi_name_index_flow import aoi_name_index_flow
from pipelines.aois.sources import aoi_prefix, aoi_sources, select_aoi_sources
from pipelines.utils import s3_uri_exists
from prefect import flow, task


@flow(name="AOI vectors")
def aoi_flow(source: AoiSource, overwrite: bool = False) -> str:
    """Writes a validated GeoParquet of one AOI dataset, with a report of every
    feature removed or repaired."""
    prefix = aoi_prefix(source)
    geoparquet_uri = f"{prefix}/aois.parquet"
    changed_aois_uri = f"{prefix}/changed_aois.csv"

    if not overwrite and s3_uri_exists(geoparquet_uri):
        return geoparquet_uri

    raw = task(source.load, name=f"load {source.source}")()
    normalized = task(stages.normalize)(raw, source)
    identified = task(stages.drop_missing_ids)(normalized)
    repaired, repair_report = task(repair_geometries)(identified)
    features = task(stages.with_area_ha)(repaired)
    changes = task(stages.changed_aois)(normalized, identified, repair_report)

    task(stages.write_csv)(changes, changed_aois_uri)
    task(stages.write_geoparquet)(features, geoparquet_uri)
    return geoparquet_uri


def aois_update(
    build_date: date,
    aoi_source: str | None = None,
    overwrite: bool = False,
    refresh_wikimedia_importance: bool = False,
    index_only: bool = False,
) -> list[str]:
    """Builds every AOI source's GeoParquet, or only the named source's, then
    the name index for that date from every source. With `index_only`, only
    the name index is built."""
    sources = aoi_sources()
    selected = () if index_only else select_aoi_sources(sources, aoi_source)
    uris = [aoi_flow(source, overwrite=overwrite) for source in selected]
    uris.append(
        aoi_name_index_flow(
            sources,
            build_date,
            overwrite=overwrite,
            refresh_wikimedia_importance=refresh_wikimedia_importance,
        )
    )
    return uris
