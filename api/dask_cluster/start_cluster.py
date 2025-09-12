import asyncio
import os

from dask_cloudprovider.aws import ECSCluster
from dask.distributed import Client


async def main():
    """Function to create dask cluster on AWS ECS."""
    cluster_arn = os.environ["DASK_CLUSTER_ARN"]
    cluster_vpc = os.environ["DASK_VPC"]
    worker_task_definition_arn = os.environ["DASK_WORKER_TASK_DEFINITION_ARN"]
    scheduler_address = os.environ["DASK_SCHEDULER_ADDRESS"]

    cluster = ECSCluster(
        cluster_arn=cluster_arn,
        # n_workers=2,
        worker_cpu=2048,
        worker_mem=8192,
        fargate_workers=True,
        vpc=cluster_vpc,
        subnets=["subnet-0f1544432f2a769d2"],
        asynchronous=True,
        skip_cleanup=True,
        worker_task_definition_arn=worker_task_definition_arn,
        scheduler_address=scheduler_address,
    )

    await cluster
    print(f"New cluster created. Scheduler: {cluster.scheduler_address}")

    client = Client(os.environ["DASK_SCHEDULER_ADDRESS"], asynchronous=True)
    await client  # connect
    await client.wait_for_workers(2)  # this one IS awaitable

    info = client.scheduler_info()  # NOT awaitable
    workers = list(info["workers"].keys())

    # retire an explicit set (no timeout kwarg here)
    await asyncio.wait_for(
        client.retire_workers(workers=workers, close_workers=True, remove=True),
        timeout=240,
    )

    cluster.adapt(minimum=2, maximum=10)

    print(f"Client connected. Dashboard: {cluster.dashboard_link}")

    return cluster, client


if __name__ == "__main__":
    asyncio.run(main())
