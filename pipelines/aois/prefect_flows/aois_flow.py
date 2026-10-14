import logging

from pipelines.aois import stages
from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.geometry_repair import repair_geometries
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import s3_uri_exists
from prefect import flow, task


@flow(name="AOI vectors")
def aoi_flow(source: AoiSource, overwrite: bool = False) -> str:
    """Build a validated GeoParquet from one AOI dataset, plus a report of any
    invalid source geometries."""
    prefix = f"s3://{ANALYTICS_BUCKET}/vectors/aois/{source.source}/{source.version}"
    geoparquet_uri = f"{prefix}/aois.parquet"
    report_uri = f"{prefix}/geometry_repair_report.csv"

    if not overwrite and s3_uri_exists(geoparquet_uri):
        return geoparquet_uri

    raw = task(stages.load_features)(source.uri, source.layer, source.where)
    normalized = task(stages.normalize)(raw, source)
    repaired, report = task(repair_geometries)(normalized)
    log_unrepaired(source, report)
    with_area = task(stages.with_area_ha)(repaired)
    features = task(stages.deduplicate)(with_area)

    task(stages.write_csv)(report, report_uri)
    task(stages.write_geoparquet)(features, geoparquet_uri)
    return geoparquet_uri


def log_unrepaired(source: AoiSource, report) -> None:
    unrepaired = report[~report.repaired]
    if not unrepaired.empty:
        logging.warning(
            "Dropped %d %s AOIs with unrepairable geometries: %s",
            len(unrepaired),
            source.source,
            unrepaired.to_dict("records"),
        )
