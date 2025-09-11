import asyncio
import os

from dask_cloudprovider.aws import ECSCluster
from dask.distributed import Client


async def main():
    """Function to create dask cluster on AWS ECS."""
    cluster_arn = os.environ["DASK_CLUSTER_ARN"]
    cluster_vpc = os.environ["DASK_VPC"]
    worker_task_definition_arn = os.environ["DASK_WORKER_TASK_DEFINITION_ARN"]
    scheduler_task_definition_arn = os.environ["DASK_SCHEDULER_TASK_DEFINITION_ARN"]

    cluster = ECSCluster(
        cluster_arn=cluster_arn,
        n_workers=2,
        worker_cpu=2048,
        worker_mem=8192,
        fargate_scheduler=True,
        fargate_workers=True,
        vpc=cluster_vpc,
        subnets=["subnet-0f1544432f2a769d2"],
        asynchronous=True,
        skip_cleanup=True,
        scheduler_timeout="30 days",
        worker_task_definition_arn=worker_task_definition_arn,
        scheduler_task_definition_arn=scheduler_task_definition_arn,
    )

    print("Waiting for new cluster to start...")
    await cluster
    print(f"New cluster created. Scheduler: {cluster.scheduler_address}")
    cluster.adapt(minimum=2, maximum=10)

    client = Client(cluster, asynchronous=True)
    await client
    print(f"Client connected. Dashboard: {cluster.dashboard_link}")

    return cluster, client


if __name__ == "__main__":
    asyncio.run(main())
