from datetime import date

from pipelines.aois.prefect_flows import aois_flow


def test_index_only_builds_the_name_index_without_any_aoi_source(monkeypatch):
    built_sources = []
    monkeypatch.setattr(
        aois_flow,
        "aoi_flow",
        lambda source, overwrite: built_sources.append(source.source),
    )
    monkeypatch.setattr(
        aois_flow,
        "aoi_name_index_flow",
        lambda sources, build_date, **options: "s3://index",
    )

    uris = aois_flow.aois_update(date(2026, 11, 4), index_only=True)

    assert built_sources == []
    assert uris == ["s3://index"]
