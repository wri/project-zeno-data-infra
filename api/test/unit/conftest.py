import dask
import pytest


@pytest.fixture(autouse=True)
def _force_local_dask_scheduler():
    """Force the synchronous scheduler for all unit tests.

    The integration conftest creates a distributed Client that registers
    itself as dask's global default. Even after the cluster shuts down,
    that stale client is still used for .compute() calls.  Setting
    scheduler="synchronous" overrides any registered client."""
    with dask.config.set(scheduler="synchronous"):
        yield
