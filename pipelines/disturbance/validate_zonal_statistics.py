import pandas as pd
import pandera.pandas as pa
from pandera.typing.pandas import Series

class ZonalStatsSchema(pa.DataFrameModel):
    country: Series[int] = pa.Field(eq=76) # gadm id for Brazil
    region: Series[int] = pa.Field(eq=20) # gadm id for AOI
    subregion: Series[int] = pa.Field(lt=170) # placeholder adm2
    alert_date: Series[int] = pa.Field(ge=731, le=1640) # julian date between 2023-01-01 to latest version
    confidence: Series[int] = pa.Field(ge=2, le=3) # low confidence, high confidence
    value: Series[int]
    
    class Config:
        coerce = True
        strict = True
        name = "ZonalStatsSchema"
        ordered = True
        unique_columns = ["country", "region", "subregion", "alert_date", "confidence"]

    @classmethod
    def calculate_alert_counts(cls, df: pd.DataFrame) -> dict:
        """Calculate the number of alerts by confidence level."""
        alert_counts = df["confidence"].value_counts().to_dict()
        return {
            "low_confidence": alert_counts.get(2, 0),
            "high_confidence": alert_counts.get(3, 0),
        }

def validates_zonal_statistics(zarr_uri: str, parquet_uri: str) -> bool:
    """Validate Zarr to confirm there's no issues with the input transformation."""
    
    # load local results
    validation_df = pd.read_csv("../notebooks/validation_stats.csv")

    # load zeno stats for aoi
    zeno_df = pd.read_parquet(parquet_uri)
    zeno_aoi_df = zeno_df[(zeno_df["country"] == 76) & (zeno_df["region"] == 20)]

    # validate zonal stats schema
    ZonalStatsSchema.validate(zeno_aoi_df)

    # validate alert counts
    validation_alerts = ZonalStatsSchema.calculate_alert_counts(validation_df)
    zeno_alerts = ZonalStatsSchema.calculate_alert_counts(zeno_aoi_df)
    assert validation_alerts == zeno_alerts, f"Alert counts do not match: {validation_alerts} != {zeno_alerts}"
    
    return True
