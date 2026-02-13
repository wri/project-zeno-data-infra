import dask
import pytest


@pytest.fixture(autouse=True)
def _clear_dask_scheduler_config():
    """Clear Dask scheduler config so unit tests use the default local
    scheduler instead of connecting to a distributed cluster."""
    with dask.config.set({"scheduler-address": None}):
        yield
