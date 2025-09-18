import pytest
from dask.distributed import LocalCluster
import os


@pytest.fixture(scope="session", autouse=True)
def dask_local_cluster():
    cluster = LocalCluster(
        n_workers=8,
        threads_per_worker=2,
        dashboard_address=None,
        processes=False,
        silence_logs=True,
    )

    os.environ["DASK_SCHEDULER_ADDRESS"] = cluster.scheduler_address
    yield cluster

    # Cleanup - use synchronous methods to avoid event loop issues
    try:
        # await dask_client.close()
        cluster.close()
    except Exception as e:
        # Ignore cleanup errors as they're often related to event loop closure
        pass
