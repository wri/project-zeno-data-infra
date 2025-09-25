===================================================
To run tests:

If you want to run the prefect server and worker tasks locally (but xarray.reduce
tasks still in coiled), from the top level of the repo do:
  `cd api`
  `uv sync`  [if not already done]
  `source .venv/bin/activate` [if not already activated]
  `AWS_PROFILE=<zeno_profile> API_KEY=<GFW Data API key> pytest test`
