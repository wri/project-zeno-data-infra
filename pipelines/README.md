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
  `AWS_PROFILE=<zeno profile> python -m pipelines.run_updates`
