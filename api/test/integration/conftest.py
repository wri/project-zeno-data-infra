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


@pytest.fixture(scope="session")
def input_uris_for():
    """Generate input URIs for an Analyzer class, for us in integration tests."""
    cache = {}

    def _get(analyzer_class):
        if analyzer_class not in cache:
            cache[analyzer_class] = analyzer_class(compute_engine=None).input_uris()
        return cache[analyzer_class]

    return _get


@pytest.fixture(scope="session")
def make_analytics_in(input_uris_for):
    def _make(analytics_class, analyzer_class, **kwargs):
        analytics_in = analytics_class(**kwargs)
        analytics_in.set_input_hash(input_uris_for(analyzer_class))
        return analytics_in

    return _make
