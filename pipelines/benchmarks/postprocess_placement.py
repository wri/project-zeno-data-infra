"""Benchmark: postprocess + save on the flow process vs on the worker holding results.

Throwaway: lives on a benchmark branch only. Runs the real DIST reduction once, keeps
its result on the cluster as a future, then times both placements from that result.
"""

import gc
import json
import shutil
import tempfile
import threading
import time
from pathlib import Path

import fsspec
import numpy as np
import pandas as pd
import psutil
import xarray as xr
from dask.distributed import get_client, wait
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce
from prefect import flow
from prefect.logging import get_run_logger

import pipelines
from pipelines.disturbance import stages as dist_stages
from pipelines.disturbance.create_zarr import create_zarr
from pipelines.disturbance.prefect_flows.gadm_dist_alerts_by_natural_lands import (
    NATURAL_LANDS_CLASSES,
)
from pipelines.globals import sbtn_natural_lands_zarr_uri
from pipelines.prefect_flows import common_stages

SCRATCH_PREFIX = "s3://lcl-analytics/scratch/postprocess-placement-test"
# output name -> (contextual column, contextual zarr, contextual expected groups)
OUTPUTS = {
    "admin-dist-alerts": None,
    "admin-dist-alerts-by-natural-land-class": (
        "natural_land_class",
        sbtn_natural_lands_zarr_uri,
        np.arange(22),
    ),
}
ADMIN_GROUPS = (np.arange(999), np.arange(86), np.arange(854))
DATE_CONFIDENCE_GROUPS = (np.arange(731, 3288), [1, 2, 3])


def lazy_reduction(dist_zarr_uri: str, contextual) -> xr.DataArray:
    """The same load, setup and flox reduce as the DIST sub-flows, left lazy."""
    name, uri, groups = contextual or (None, None, None)
    expected_groups = ADMIN_GROUPS + ((groups,) if contextual else ())
    expected_groups += DATE_CONFIDENCE_GROUPS
    datasets = dist_stages.load_data(dist_zarr_uri, uri)
    reduce_mask, groupbys, expected_groups = dist_stages.setup_compute(
        datasets, expected_groups, name
    )
    return xarray_reduce(
        reduce_mask,
        *groupbys,
        func="sum",
        expected_groups=expected_groups,
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0,
    )


def postprocess(result: xr.DataArray, contextual_name) -> pd.DataFrame:
    df = dist_stages.create_result_dataframe(result)
    if contextual_name == "natural_land_class":
        df[contextual_name] = (
            df[contextual_name].map(NATURAL_LANDS_CLASSES).fillna("Unclassified")
        )
    return df


def postprocess_and_save(result: xr.DataArray, contextual_name, uri: str) -> dict:
    t0 = time.perf_counter()
    df = postprocess(result, contextual_name)
    t1 = time.perf_counter()
    common_stages.save_results(df, uri)
    t2 = time.perf_counter()
    return {"rows": len(df), "postprocess_s": t1 - t0, "save_s": t2 - t1}


class PeakRss:
    """Peak resident memory of this process above the level at entry."""

    def __enter__(self):
        gc.collect()
        self._proc = psutil.Process()
        self.base = self._proc.memory_info().rss
        self.peak = self.base
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def _sample(self):
        while not self._done.is_set():
            self.peak = max(self.peak, self._proc.memory_info().rss)
            time.sleep(0.01)

    def __exit__(self, *exc):
        self._done.set()
        self._thread.join()
        self.extra_gb = (self.peak - self.base) / 1e9


def upload_this_code(client) -> None:
    """Ship this branch's `pipelines` package; cluster images may predate it."""
    package_dir = Path(pipelines.__file__).parent
    staging = Path(tempfile.mkdtemp())
    shutil.copytree(
        package_dir,
        staging / "pipelines",
        # "pipelines": nested copy left by Prefect's flow-code download
        ignore=shutil.ignore_patterns(
            ".venv", "__pycache__", "test", "*.png", "uv.lock", "pipelines"
        ),
    )
    archive = shutil.make_archive(
        str(staging / "pipelines_code"), "zip", staging, "pipelines"
    )
    client.upload_file(archive)


def _timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t0


@flow(name="Postprocess placement benchmark", log_prints=True)
def postprocess_placement_benchmark(
    version="v20260919",
    overwrite=False,
    is_latest=False,
    outputs=("admin-dist-alerts-by-natural-land-class",),
    scratch_prefix=SCRATCH_PREFIX,
):
    logger = get_run_logger()
    client = get_client()
    upload_this_code(client)
    version = version or "v20260919"  # run_updates passes version=None when unset
    dist_zarr_uri = create_zarr(version, overwrite=False)
    results = {}

    for name in outputs:
        contextual = OUTPUTS[name]
        contextual_name = contextual[0] if contextual else None

        # Real reduction; the result stays on the worker that finished it.
        t0 = time.perf_counter()
        result_fut = client.compute(lazy_reduction(dist_zarr_uri, contextual))
        wait(result_fut)
        t_reduce = time.perf_counter() - t0
        nbytes = client.submit(lambda r: r.data.nbytes, result_fut).result()
        logger.info(
            f"{name}: reduction finished in {t_reduce / 60:.1f} min, result kept "
            f"on the cluster ({nbytes / 1e9:.2f} GB)"
        )

        # Worker placement: postprocess + write where the result lives;
        # only a summary returns.
        with PeakRss() as mem_worker:
            worker_stats, t_worker = _timed(
                lambda: client.submit(
                    postprocess_and_save,
                    result_fut,
                    contextual_name,
                    f"{scratch_prefix}/{name}.worker.parquet",
                ).result()
            )

        # Current placement: gather to the flow process, postprocess, write,
        # then read back as validation does.
        flow_uri = f"{scratch_prefix}/{name}.flow.parquet"
        with PeakRss() as mem_flow:
            t0 = time.perf_counter()
            result = result_fut.result()
            t1 = time.perf_counter()
            df = postprocess(result, contextual_name)
            t2 = time.perf_counter()
            common_stages.save_results(df, flow_uri)
            t3 = time.perf_counter()
            del result
            pd.read_parquet(flow_uri)
            t4 = time.perf_counter()
            rows = len(df)
            del df
            gc.collect()
        t_gather, t_post, t_save, t_readback = t1 - t0, t2 - t1, t3 - t2, t4 - t3

        results[name] = {
            "version": version,
            "rows": rows,
            "reduce_s": t_reduce,
            "sparse_result_gb": nbytes / 1e9,
            "flow": {
                "gather_s": t_gather,
                "postprocess_s": t_post,
                "save_s": t_save,
                "validation_readback_s": t_readback,
                "total_s": t_gather + t_post + t_save + t_readback,
                "peak_extra_memory_gb": mem_flow.extra_gb,
            },
            "worker": {
                "total_s": t_worker,
                **{f"on_worker_{k}": v for k, v in worker_stats.items()},
                "peak_extra_memory_gb": mem_worker.extra_gb,
            },
        }
        logger.info(f"{name}: {json.dumps(results[name], indent=2)}")
        client.cancel(result_fut)

    fs, root = fsspec.core.url_to_fs(scratch_prefix)
    with fs.open(f"{root}/results.json", "w") as f:
        json.dump(results, f, indent=2)
    for path in fs.glob(f"{root}/*.parquet"):
        fs.rm(path, recursive=True)
    return [f"{scratch_prefix}/results.json"]
