import logging
import tempfile
from pathlib import Path

from pipelines.concessions import stages
from pipelines.concessions.concession_source import ConcessionSource
from pipelines.concessions.geometry_repair import repair_geometries
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import s3_uri_exists
from prefect import flow, task


@flow(name="Concessions vectors")
def concessions_flow(
    source: ConcessionSource, version: str, overwrite: bool = False
) -> list[str]:
    """Build a GeoParquet (for analysis) and PMTiles (for maps) from a
    concession dataset, plus a report of any invalid source geometries."""
    prefix = (
        f"s3://{ANALYTICS_BUCKET}/vectors/concessions/"
        f"{source.concession_type}/{version}"
    )
    geoparquet_uri = f"{prefix}/concessions.parquet"
    pmtiles_uri = f"{prefix}/concessions.pmtiles"
    report_uri = f"{prefix}/geometry_repair_report.csv"
    result_uris = [geoparquet_uri, pmtiles_uri, report_uri]

    if not overwrite and s3_uri_exists(geoparquet_uri) and s3_uri_exists(pmtiles_uri):
        return result_uris

    raw = task(stages.load_source)(source.uri_for(version))
    normalized = task(stages.normalize)(raw, source)
    repaired, report = task(repair_geometries)(normalized)
    log_unrepaired(report)
    features = task(stages.with_area_ha)(repaired)

    task(stages.write_csv)(report, report_uri)
    task(stages.write_geoparquet)(features, geoparquet_uri)
    with tempfile.TemporaryDirectory() as directory:
        pmtiles_path = task(stages.build_pmtiles)(
            features, Path(directory) / "concessions.pmtiles", layer="concessions"
        )
        task(stages.upload_file)(pmtiles_path, pmtiles_uri)

    return result_uris


def log_unrepaired(report) -> None:
    unrepaired = report[~report.repaired]
    if not unrepaired.empty:
        logging.warning(
            "Dropped %d concessions with unrepairable geometries: %s",
            len(unrepaired),
            unrepaired.to_dict("records"),
        )
