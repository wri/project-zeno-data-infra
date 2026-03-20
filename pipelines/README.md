# Pipelines

## Running Tests

```bash
cd pipelines
uv sync            # if not already done
source .venv/bin/activate  # if not already activated
AWS_PROFILE=<GFW Data API production profile> pytest test
```

## Running Pipeline Code Locally

### AWS Permissions

Pipelines interact with S3 resources in the Zeno AWS account. Make sure you have a valid AWS profile configured with the necessary permissions before running any pipeline:

```bash
export AWS_PROFILE=<zeno-profile>
```

### Coiled (Dask Cluster)

Pipeline zonal stats computations run on a Dask cluster managed by [Coiled](https://cloud.coiled.io/). If not already logged in, authenticate from the terminal:

```bash
coiled login
```

Coiled syncs your local Python environment to the remote Dask scheduler and workers. Before running a pipeline, make sure:

1. **Activate the pipelines virtual environment** — the Coiled cluster is provisioned using packages from your active environment.
2. **Keep the environment up to date** — run `uv sync` so the packages Coiled syncs to the cluster match what the pipeline code expects.

```bash
cd pipelines
uv sync
source .venv/bin/activate
```

> **⚠ Architecture note:** The Coiled cluster in `run_updates.py` uses ARM-based instances (`r7g` family). If your local machine is not ARM (e.g. Intel/AMD x86_64), you may encounter package incompatibilities when Coiled attempts to sync your environment. Running from an Apple Silicon Mac or another ARM host avoids this issue.

### Prefect

To monitor pipeline flows via the Prefect UI, there are two options depending on what's configured in `~/.prefect/profiles.toml`:

#### Option 1: Local Prefect Server

Spin up a local Prefect server in a separate terminal session:

```bash
prefect server start
```

The Prefect dashboard will be available at http://127.0.0.1:4200 and local pipeline runs will show up there.

#### Option 2: Prefect Cloud

If the active profile in `~/.prefect/profiles.toml` points to a Prefect Cloud workspace, runs will appear in the cloud dashboard instead. Example `profiles.toml`:

```toml
active = "local"

[profiles.local]
PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>"
PREFECT_API_KEY = "*********"
```

### Running a Pipeline

Pipelines are launched via the Click CLI in `run_updates.py`. Run with `--help` to see all available options:

```bash
python -m pipelines.run_updates --help
```

```
Usage: python -m pipelines.run_updates [OPTIONS]

Options:
  --flow [dist_update|tcl_update]
                                  Which update flow to run.
  --version TEXT                  Dataset version (required for tcl_update).
  --overwrite                     Overwrite existing outputs.
  --is-latest                     Mark this version as latest.
  --help                          Show this message and exit.
```

Example — run the DIST alerts update:

```bash
python -m pipelines.run_updates --flow dist_update --version v20260301
```

## Running Pipeline from Prefect Cloud UI

<!-- TODO: Add details for running pipeline from Prefect Cloud UI -->
