import geopandas as gpd


class QCFeaturesRepository:
    QC_FEATURES_URI = "s3://lcl-analytics/vectors/qc_features.geojson"

    def load(self, aoi_id=None, aoi_type=None):
        qc_features = gpd.read_file(self.QC_FEATURES_URI)

        if aoi_id is not None and aoi_type is not None:
            return qc_features[
                (qc_features["aoi_id"] == aoi_id)
                & (qc_features["aoi_type"] == aoi_type)
            ]

        return qc_features
