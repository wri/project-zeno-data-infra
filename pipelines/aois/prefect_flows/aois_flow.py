import logging

from prefect import flow, task

from pipelines.aois import stages
from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.geometry_repair import repair_geometries
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import s3_uri_exists


@flow(name="AOI vectors")
def aoi_flow(source: AoiSource, overwrite: bool = False) -> str:
    """Build a validated GeoParquet from one AOI dataset, plus a report of any
    invalid source geometries."""
    prefix = f"s3://{ANALYTICS_BUCKET}/vectors/aois/{source.source}/{source.version}"
    geoparquet_uri = f"{prefix}/aois.parquet"
    report_uri = f"{prefix}/geometry_repair_report.csv"

    if not overwrite and s3_uri_exists(geoparquet_uri):
        return geoparquet_uri

    raw = task(stages.load_features)(source.uri, source.layer)
    normalized = task(stages.normalize)(raw, source)
    repaired, report = task(repair_geometries)(normalized)
    log_unrepaired(source, report)
    with_area = task(stages.with_area_ha)(repaired)
    deduplicated = task(stages.deduplicate)(with_area)
    features = task(stages.with_extent)(deduplicated)

    task(stages.write_csv)(report, report_uri)
    task(stages.write_geoparquet)(features, geoparquet_uri)
    return geoparquet_uri


@flow(name="AOI names")
def aoi_names_flow(
    geoparquet_uris: list[str],
    columns: list[str],
    version: str,
    overwrite: bool = False,
) -> str:
    """Combine the searchable, non-geometry columns of every AOI dataset into
    one parquet for name search."""
    uri = f"s3://{ANALYTICS_BUCKET}/vectors/aois/names/{version}/aoi_names.parquet"
    if not overwrite and s3_uri_exists(uri):
        return uri

    names = task(stages.build_search_index)(geoparquet_uris, columns)
    task(stages.write_parquet)(names, uri)
    return uri


def log_unrepaired(source: AoiSource, report) -> None:
    unrepaired = report[~report.repaired]
    if not unrepaired.empty:
        logging.warning(
            "Dropped %d %s AOIs with unrepairable geometries: %s",
            len(unrepaired),
            source.source,
            unrepaired.to_dict("records"),
        )
