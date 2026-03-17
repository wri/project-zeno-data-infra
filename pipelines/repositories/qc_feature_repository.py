import geopandas as gpd

from pipelines.globals import ANALYTICS_BUCKET


class QCFeaturesRepository:
    QC_FEATURES_URI = f"s3://{ANALYTICS_BUCKET}/vectors/qc_features.geojson"

    def load(self, start=0, limit=None, aoi_id=None, aoi_type=None):
        # 'start' specified where to start in the feature list, 'limit' where to end.
        # Or you can choose a specific feature by GID_2 value.
        qc_features = gpd.read_file(self.QC_FEATURES_URI)

        if aoi_id is not None:
            return qc_features[
                (qc_features["GID_2"] == aoi_id)
            ]
        elif limit is not None:
            return qc_features[start:limit]

        return qc_features

    def write_results(self, results: gpd.GeoDataFrame, analysis: str, version: str):
        output_uri = (
            f"s3://{ANALYTICS_BUCKET}/zonal-statistics/{version}/{analysis}-qc.geojson"
        )
        results.to_file(output_uri, index=False)
