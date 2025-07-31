## gfw-data-lake file naming for Zeno

Here is the current naming schema for files generated on gfw-data-lake for Zeno.

We are transforming the input raster data into zarrs.  The zarrs should be stored under
the `raster/epsg-4326/zarr` path under the dataset/version that it was created from.
The dataset name can be repeated in the base name of the zarr, but there is no need
to repeat the version.  So, the template for a zarr name would be:
```
  s3://gfw-data-lake/{dataset}/{version}/raster/epsg-4326/zarr/{dataset}.zarr/
```
and a concrete example would be:
```
  s3://gfw-data-lake/umd_glad_dist_alerts/v20250723/raster/epsg-4326/zarr/umd_glad_dist_alerts.zarr/
```

The output results that we are serving to Zeno is zonal statistics in a tabular form,
stored in a parquet file.  The parquet files should be stored under the
`tabular/statistics` path under the dataset/version that it was mainly based on. The
base name of the parquet file should include the type of area of interest (such as
"admin" for gadm areas), and any layers that the main dataset was intersected with.
So, the template for a parquet
name would be:
```
  s3://gfw-data-lake/{dataset}/{version}/tabular/statistics/{aoitype}_by_{intersection}.parquet
```
and a concrete example would be:
```
  s3://gfw-data-lake/umd_glad_dist_alerts/v20250723/tabular/statistics/admin_by_drivers.parquet
```

or

```
  s3://gfw-data-lake/sbtn_natural_lands/v2025/tabular/statistics/admin.parquet
```
