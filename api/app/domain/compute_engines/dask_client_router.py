import logging
import os

logger = logging.getLogger(__name__)

# Total area threshold (hectares) below which the local cluster is used.
# Configurable via environment variable; defaults to 50 million ha.
LOCAL_CLUSTER_AREA_THRESHOLD_HA = float(
    os.environ.get("LOCAL_CLUSTER_AREA_THRESHOLD_HA", 50_000_000)
)


class DaskClientRouter:
    """Selects a local or remote dask client based on total AOI area."""

    def __init__(self, local_client, remote_client, threshold_ha=None):
        self.local_client = local_client
        self.remote_client = remote_client
        self.threshold_ha = (
            threshold_ha
            if threshold_ha is not None
            else LOCAL_CLUSTER_AREA_THRESHOLD_HA
        )

    def get_client(self, total_area_ha: float):
        if total_area_ha <= self.threshold_ha:
            logger.info(
                "Routing to local dask cluster (total_area_ha=%.2f, threshold=%.2f)",
                total_area_ha,
                self.threshold_ha,
            )
            return self.local_client
        logger.info(
            "Routing to remote dask cluster (total_area_ha=%.2f, threshold=%.2f)",
            total_area_ha,
            self.threshold_ha,
        )
        return self.remote_client
