import pytest
from dask.distributed import LocalCluster
import os


@pytest.fixture(scope="session", autouse=True)
def dask_local_cluster():
    cluster = LocalCluster(
        n_workers=8,
        threads_per_worker=2,
        dashboard_address=None,
        silence_logs=True,
    )

    os.environ["DASK_SCHEDULER_ADDRESS"] = cluster.scheduler_address
    yield cluster

    cluster.close()
