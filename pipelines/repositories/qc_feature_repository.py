import geopandas as gpd

from pipelines.globals import ANALYTICS_BUCKET


class QCFeaturesRepository:
    QC_FEATURES_URI = f"s3://{ANALYTICS_BUCKET}/vectors/qc_features.geojson"

    def load(self, limit=None, aoi_id=None, aoi_type=None):
        qc_features = gpd.read_file(self.QC_FEATURES_URI)

        if aoi_id is not None and aoi_type is not None:
            return qc_features[
                (qc_features["aoi_id"] == aoi_id)
                & (qc_features["aoi_type"] == aoi_type)
            ]
        elif limit is not None:
            return qc_features[:limit]

        return qc_features

    def write_results(self, results: gpd.GeoDataFrame, analysis: str, version: str):
        output_uri = (
            f"s3://{ANALYTICS_BUCKET}/zonal-statistics/{version}/{analysis}-qc.geojson"
        )
        results.to_file(output_uri, index=False)
