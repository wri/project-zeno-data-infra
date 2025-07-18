
Follow these instructions to get your dataset on Zeno:

- Run github coddespace
- From codespace, run `coiled notebook start`
- Open `Zeno COG` notebook and follow instructions in cells to publish a draft dataset
- If you to calculate global GADM  statistics, open the `Zeno GADM statistics` notebook and follow the instructions to publish a draft dataset
- Review and QC your datasets. You can use the `QC data` notebook to find some helpful tools.
- Open `Publish datasets to Zeno` notebook and follow instructions to release your dataset

===================================================

If you want to run the prefect server and worker tasks locally (but xarray.reduce
tasks still in coiled), do:
  `uv sync`  [if not already done]
  `coiled login`
  `source .venv/bin/activate` [if not already activated]
  `prefect server start` (in a separate window, also activated)
  `AWS_PROFILE=gfw-production python -m pipelines.dist_flow`

If you want to run the prefect server in Prefect cloud, and the worker tasks and
xarray.reduce tasks in coiled, do:
  `uv sync`  [if not already done]
  `coiled login`
  `prefect login` [log into cloud account using a token]
  `prefect --no-prompt deploy --prefect-file prefect.yaml --all` [create a "deployment"]
  `prefect deployment run 'myflow/deploy1'` [start a run using that deployment]

This deploys your current code and environment into prefect-based Docker that runs
both the worker tasks and xarray reduce tasks. This is a shared deployment called
'deploy1', unless you change the name by editing the prefect.yaml file.
