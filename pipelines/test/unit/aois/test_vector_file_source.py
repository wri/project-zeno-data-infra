import pandas as pd

from pipelines.aois.vector_file_source import VectorFileSource


def test_missing_ids_stay_missing_instead_of_becoming_strings():
    source = VectorFileSource(
        source="kba", version="v1", uri="s3://bucket/kba.zip", id_column="sitrecid"
    )
    attributes = pd.DataFrame({"sitrecid": [101, None]})

    assert source.aoi_ids(attributes).tolist() == ["101", pd.NA]
