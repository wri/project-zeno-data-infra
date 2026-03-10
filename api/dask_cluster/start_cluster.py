import logging
import os
import time

from dask.distributed import Client
from dask_cloudprovider.aws import ECSCluster, EC2Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_cluster_manager():
    # cluster_arn = os.environ["DASK_CLUSTER_ARN"]
    cluster_vpc = os.environ["DASK_VPC"]
    # worker_task_definition_arn = os.environ["DASK_WORKER_TASK_DEFINITION_ARN"]
    # scheduler_address = os.environ["DASK_SCHEDULER_ADDRESS"]
    security_group_id = os.environ["DASK_WORKER_SECURITY_GROUP"]
    minimum = int(os.environ.get("DASK_MIN_WORKERS", "2"))
    maximum = int(os.environ.get("DASK_MAX_WORKERS", "30"))

    logger.info(f"Starting cluster manager - min: {minimum}, max: {maximum}")

    # cluster = ECSCluster(
    #     cluster_arn=cluster_arn,
    #     vpc=cluster_vpc,
    #     subnets=["subnet-0f1544432f2a769d2"],
    #     worker_task_definition_arn=worker_task_definition_arn,
    #     scheduler_address=scheduler_address,
    #     security_groups=[security_group_id],
    #     fargate_workers=True,
    #     worker_cpu=8192,
    #     worker_mem=32768,
    #     skip_cleanup=True,
    #     shutdown_on_close=False,
    # )

    cluster = EC2Cluster(
        vpc=cluster_vpc,
        worker_instance_type="r7i.2xlarge",
        scheduler_instance_type="r7i.xlarge",
        subnet_id="subnet-0f1544432f2a769d2",
        security_groups=[security_group_id],
        filesystem_size=50,
        n_workers=2,
        env_vars={
            "AWS_ACCESS_KEY_ID": os.getenv(
                "AWS_ACCESS_KEY_ID",
            ),
            "AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY",
            ),
            "DASK_TEMPORARY_DIRECTORY": "/tmp/dask",
        },
        # ami="ami-00cdb36f35bd8af7d",
        ami="ami-09252de1c909bd925",  # slimmed down
        key_name="solomon_dask_test",
        iam_instance_profile={"Name": "analytics-ec2-role"},
        bootstrap=True,
        debug=False,
        shutdown_on_close=True,
        extra_bootstrap=[
            "aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 084375562450.dkr.ecr.us-east-1.amazonaws.com",
        ],
        scheduler_options={"idle_timeout": "30 minutes"},
        security=False,
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
