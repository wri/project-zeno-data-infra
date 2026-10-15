import logging

import pandas as pd
from pipelines.aois import stages
from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.geometry_repair import repair_geometries
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import s3_uri_exists
from prefect import flow, task


@flow(name="AOI vectors")
def aoi_flow(source: AoiSource, overwrite: bool = False) -> str:
    """Build a validated GeoParquet from one AOI dataset, plus reports of the
    invalid source geometries and of every feature left out."""
    prefix = f"s3://{ANALYTICS_BUCKET}/vectors/aois/{source.source}/{source.version}"
    geoparquet_uri = f"{prefix}/aois.parquet"
    repair_report_uri = f"{prefix}/geometry_repair_report.csv"
    removed_report_uri = f"{prefix}/removed_features.csv"

    if not overwrite and s3_uri_exists(geoparquet_uri):
        return geoparquet_uri

    raw = task(source.load, name=f"load {source.source}")()
    normalized = task(stages.normalize)(raw, source)
    identified = task(stages.drop_missing_ids)(normalized)
    repaired, repair_report = task(repair_geometries)(identified)
    log_unrepaired(source, repair_report)
    with_area = task(stages.with_area_ha)(repaired)
    features = task(stages.deduplicate)(with_area)
    removed_features = pd.concat(
        [
            stages.removed(normalized, identified, "missing_id"),
            stages.removed(identified, repaired, "unrepairable_geometry"),
            stages.removed(with_area, features, "duplicate_id"),
        ],
        ignore_index=True,
    )

    task(stages.write_csv)(repair_report, repair_report_uri)
    task(stages.write_csv)(removed_features, removed_report_uri)
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
