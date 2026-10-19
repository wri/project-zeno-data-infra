from prefect import flow, task

from pipelines.aois import stages
from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.geometry_repair import repair_geometries
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import s3_uri_exists


@flow(name="AOI vectors")
def aoi_flow(source: AoiSource, overwrite: bool = False) -> str:
    """Writes a validated GeoParquet of one AOI dataset, with a report of every
    feature removed or repaired."""
    prefix = f"s3://{ANALYTICS_BUCKET}/vectors/aois/{source.source}/{source.version}"
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
