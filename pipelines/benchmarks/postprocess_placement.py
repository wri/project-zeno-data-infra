"""Benchmark: postprocess + save on the flow process vs on the worker holding results.

Throwaway: lives on a benchmark branch only. Rebuilds real DIST reduction results on
a worker from published parquet, then times both placements end to end.
"""

import gc
import json
import shutil
import tempfile
import threading
import time
from datetime import date
from pathlib import Path

import fsspec
import numpy as np
import pandas as pd
import psutil
import sparse
import xarray as xr
from dask.distributed import get_client, wait
from prefect import flow
from prefect.logging import get_run_logger

import pipelines
from pipelines.disturbance import stages as dist_stages
from pipelines.disturbance.prefect_flows.gadm_dist_alerts_by_natural_lands import (
    NATURAL_LANDS_CLASSES,
)
from pipelines.prefect_flows import common_stages

SOURCE_PREFIX = "s3://lcl-analytics/zonal-statistics/dist-alerts/v20260912"
SCRATCH_PREFIX = "s3://lcl-analytics/scratch/postprocess-placement-test"
OUTPUTS = {
    "admin-dist-alerts": None,
    "admin-dist-alerts-by-natural-land-class": "natural_land_class",
}
COORD_VALUES = {
    "country": np.arange(999),
    "region": np.arange(86),
    "subregion": np.arange(854),
    "natural_land_class": np.arange(22),
    "alert_date": np.arange(731, 3288),
    "confidence": np.array([1, 2, 3]),
}


def rebuild_result(parquet_uri: str, contextual) -> xr.DataArray:
    """Invert DIST postprocessing to recover the sparse array flox returns."""
    df = pd.read_parquet(parquet_uri)
    alpha3_to_numeric = {v: k for k, v in common_stages.numeric_to_alpha3.items()}
    dims = ["country", "region", "subregion"]
    dims += [contextual] if contextual else []
    dims += ["alert_date", "confidence"]
    days = (
        pd.to_datetime(df.dist_alert_date) - pd.Timestamp(date(2020, 12, 31))
    ).dt.days
    idx = {
        "country": df.country.map(alpha3_to_numeric).to_numpy(),
        "region": df.region.to_numpy(),
        "subregion": df.subregion.to_numpy(),
        "alert_date": days.to_numpy() - 731,
        "confidence": df.dist_alert_confidence.map({"low": 2, "high": 3}).to_numpy()
        - 1,
    }
    if contextual == "natural_land_class":
        codes = {v: k for k, v in NATURAL_LANDS_CLASSES.items()}
        idx[contextual] = df[contextual].map(codes).fillna(0).astype(int).to_numpy()
    coo = sparse.COO(
        np.stack([idx[d] for d in dims]),
        df.area_ha.to_numpy(),
        shape=tuple(len(COORD_VALUES[d]) for d in dims),
    )
    return xr.DataArray(coo, dims=dims, coords={d: COORD_VALUES[d] for d in dims})


def postprocess(result: xr.DataArray, contextual) -> pd.DataFrame:
    df = dist_stages.create_result_dataframe(result)
    if contextual == "natural_land_class":
        df[contextual] = (
            df[contextual].map(NATURAL_LANDS_CLASSES).fillna("Unclassified")
        )
    return df


def postprocess_and_save(result: xr.DataArray, contextual, uri: str) -> dict:
    t0 = time.perf_counter()
    df = postprocess(result, contextual)
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
    version=None,
    overwrite=False,
    is_latest=False,
    outputs=None,
    scratch_prefix=SCRATCH_PREFIX,
):
    logger = get_run_logger()
    client = get_client()
    upload_this_code(client)
    results = {}

    for name in outputs or list(OUTPUTS):
        contextual = OUTPUTS[name]
        result_fut = client.submit(
            rebuild_result, f"{SOURCE_PREFIX}/{name}.parquet", contextual
        )
        _, t_rebuild = _timed(lambda: wait(result_fut))
        nbytes = client.submit(lambda r: r.data.nbytes, result_fut).result()
        logger.info(
            f"{name}: sparse result rebuilt on worker in {t_rebuild:.0f}s "
            f"({nbytes / 1e9:.2f} GB)"
        )

        # Worker placement: postprocess + write where the result lives;
        # only a summary returns.
        with PeakRss() as mem_worker:
            worker_stats, t_worker = _timed(
                lambda: client.submit(
                    postprocess_and_save,
                    result_fut,
                    contextual,
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
            df = postprocess(result, contextual)
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
            "rows": rows,
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
                **{f"on_worker_{k}": v for k, v in worker_stats.items() if k != "rows"},
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
