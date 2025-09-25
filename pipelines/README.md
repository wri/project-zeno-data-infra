===================================================

To run tests:

  `cd pipelines`
  `uv sync`  [if not already done]
  `source .venv/bin/activate` [if not already activated]
  `AWS_PROFILE=<GFW Data API production profile> pytest test`

===================================================

If you want to run the prefect server and worker tasks locally (but with
xarray.reduce tasks still in coiled), from the top level of the repo do:
  `cd pipelines`
  `uv sync`  [if not already done]
  `source .venv/bin/activate` [if not already activated]
  `coiled login`
  `cd ..`
  `prefect server start` (in a separate window, also activated)
  `AWS_PROFILE=gfw-production python -m pipelines.dist_flow`

If you want to run the prefect server in Prefect cloud, and the worker tasks
and xarray.reduce tasks in coiled, from the top level of the repo do:
  `cd pipelines`
  `uv sync`  [if not already done]
  `source .venv/bin/activate` [if not already activated]
  `coiled login`
  `cd ..`
  `prefect login` [log into cloud account using a token]
  `prefect --no-prompt deploy --prefect-file prefect.yaml --all` [creates a "deployment"]
  `prefect deployment run 'myflow/deploy1'` [starts a run using that deployment]

This deploys your current code and environment into prefect-based Docker that runs
both the worker tasks and xarray reduce tasks. This is a shared deployment called
'deploy1', unless you change the name by editing the prefect.yaml file.
