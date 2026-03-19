import os

import pytest
from dask.distributed import LocalCluster


@pytest.fixture(scope="session", autouse=True)
def dask_test_cluster():
    """Create a single dask cluster shared across all integration tests.

    Sets DASK_SCHEDULER_ADDRESS so the app lifespan connects to this
    cluster instead of spawning new workers on every test.  LOCAL_DASK_WORKERS=0
    tells the lifespan to skip its own LocalCluster creation.
    """
    cluster = LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        dashboard_address=None,
    )

    os.environ["DASK_SCHEDULER_ADDRESS"] = cluster.scheduler_address
    os.environ["LOCAL_DASK_WORKERS"] = "0"

    yield cluster

    cluster.close()
    os.environ.pop("DASK_SCHEDULER_ADDRESS", None)
    os.environ.pop("LOCAL_DASK_WORKERS", None)
