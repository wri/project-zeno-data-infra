from .integrated_alerts import create

create.__doc__ = """
    Integrated disturbance alerts analytics.

    Returns alert area (hectares) grouped by alert date and confidence
    (low/high) for the requested AOI and date range.

    For GADM admin AOIs, results are served from precomputed zonal statistics.
    For all other AOI types, statistics are computed on-the-fly.
"""
