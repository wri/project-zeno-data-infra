import time
import os
import logging
from dask_cloudprovider.aws import ECSCluster
from dask.distributed import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_cluster_manager():
    cluster_arn = os.environ["DASK_CLUSTER_ARN"]
    cluster_vpc = os.environ["DASK_VPC"]
    worker_task_definition_arn = os.environ["DASK_WORKER_TASK_DEFINITION_ARN"]
    scheduler_address = os.environ["DASK_SCHEDULER_ADDRESS"]
    security_group_id = os.environ["DASK_WORKER_SECURITY_GROUP"]
    minimum = int(os.environ.get("DASK_MIN_WORKERS", "2"))
    maximum = int(os.environ.get("DASK_MAX_WORKERS", "10"))

    logger.info(f"Starting cluster manager - min: {minimum}, max: {maximum}")

    cluster = ECSCluster(
        cluster_arn=cluster_arn,
        vpc=cluster_vpc,
        subnets=["subnet-0f1544432f2a769d2"],
        worker_task_definition_arn=worker_task_definition_arn,
        scheduler_address=scheduler_address,
        security_groups=[security_group_id],
        fargate_workers=True,
        worker_cpu=8192,
        worker_mem=32768,
        skip_cleanup=True,
        shutdown_on_close=False,
    )

    logger.info(f"Cluster ready: {cluster.scheduler_address}")

    client = Client(cluster.scheduler_address)

    info = client.scheduler_info()
    workers = list(info["workers"])
    if workers:
        logger.info(f"Retiring {len(workers)} existing workers")
        client.retire_workers(workers=workers, close_workers=True, remove=True)

    # Enable adaptive scaling
    cluster.adapt(minimum=minimum, maximum=maximum)

    logger.info(f"Adaptive scaling active! Dashboard: {cluster.dashboard_link}")

    while True:
        time.sleep(60)
        try:
            worker_count = len(client.scheduler_info()["workers"])
            logger.info(f"Manager running - {worker_count} workers")
        except Exception as e:
            logger.warning(f"Error checking workers: {e}")


if __name__ == "__main__":
    run_cluster_manager()
