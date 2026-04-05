from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon

from pipelines.globals import (
    ANALYTICS_BUCKET,
    DATA_LAKE_BUCKET,
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows.common_stages import (
    _load_zarr,
)
from pipelines.prefect_flows.common_stages import create_zarrs as common_create_zarrs
from pipelines.prefect_flows.common_stages import (
    numeric_to_alpha3,
    rollup_by_gadm_and_convert_to_aoi,
)
from pipelines.repositories.google_earth_engine_dataset_repository import (
    GoogleEarthEngineDatasetRepository,
)
from pipelines.repositories.qc_feature_repository import QCFeaturesRepository

PIPELINE_CHUNK_SIZE = 10_000
OTF_CHUNK_SIZE = 4_000

TCL_VERSION = "v1.13"
TCLF_VERSION = "v1.13"
DRIVERS_VERSION = "v1.13"

DATASETS = {
    "tree_cover_loss": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/umd_tree_cover_loss/{TCL_VERSION}/raster/epsg-4326/10/40000/year/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/umd-tree-cover-loss/{TCL_VERSION}/year.zarr",
        "dtype": "uint8",
    },
    "tree_cover_loss_from_fires": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/umd_tree_cover_loss_from_fires/{TCLF_VERSION}/raster/epsg-4326/10/40000/year/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/umd-tree-cover-loss-from-fires/{TCLF_VERSION}/year.zarr",
        "dtype": "uint8",
    },
    "drivers": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/wri_google_tree_cover_loss_drivers/{DRIVERS_VERSION}/raster/epsg-4326/10/40000/category/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/wri-google-tree-cover-loss-drivers/{DRIVERS_VERSION}/category.zarr",
        "dtype": "uint8",
    },
}

# tcd threshold mapping
thresh_to_pct = {
    0: "0",
    1: "10",
    2: "15",
    3: "20",
    4: "25",
    5: "30",
    6: "50",
    7: "75",
}


def create_zarrs(overwrite: bool = False) -> Dict[str, str]:
    """Create zarr stores for TCL pipeline datasets from tiled GeoTIFFs."""
    return common_create_zarrs(
        datasets=DATASETS,
        overwrite=overwrite,
        pipeline_chunk_size=PIPELINE_CHUNK_SIZE,
        otf_chunk_size=OTF_CHUNK_SIZE,
    )


def load_data(
    tree_cover_loss_uri: str,
    pixel_area_uri: Optional[str] = None,
    carbon_emissions_uri: Optional[str] = None,
    tree_cover_density_uri: Optional[str] = None,
    ifl_uri: Optional[str] = None,
    drivers_uri: Optional[str] = None,
    primary_forests_uri: Optional[str] = None,
    natural_forests_uri: Optional[str] = None,
    tree_cover_loss_from_fires_uri: Optional[str] = None,
    mangrove_stock_2000_uri: Optional[str] = None,
    tree_cover_gain_from_height_uri: Optional[str] = None,
    bbox: Optional[Polygon] = None,
    group: Optional[str] = None,
) -> Tuple[
    xr.DataArray,
    xr.Dataset,
    xr.DataArray,
    xr.DataArray,
    xr.DataArray,
    xr.DataArray,
    xr.DataArray,
    xr.DataArray,
    xr.DataArray,
    xr.DataArray,
]:
    """
    Load in the tree cover loss zarr, pixel area zarr, carbon emissions zarr, tree cover density zarr, and the GADM zarrs
    Returns xr.DataArray for TCL and contextual layers and xr.Dataset for pixel area/carbon emissions
    """

    tcl: xr.DataArray = _load_zarr(tree_cover_loss_uri, group=group).band_data
    if bbox is not None:
        min_x, min_y, max_x, max_y = bbox.bounds
        # TODO assumption about zarr coords, wrap in class
        tcl = tcl.sel(x=slice(min_x, max_x), y=slice(max_y, min_y))

    # load and align zarrs with tcl

    # aggregation layers
    pixel_area: xr.DataArray = _load_zarr(pixel_area_uri).band_data
    pixel_area = xr.align(
        tcl,
        pixel_area.reindex_like(tcl, method="nearest", tolerance=1e-5),
        join="left",
    )[1]

    carbon_emissions: xr.DataArray = _load_zarr(
        carbon_emissions_uri, group=group
    ).band_data
    carbon_emissions = xr.align(
        tcl,
        carbon_emissions.reindex_like(tcl, method="nearest", tolerance=1e-5),
        join="left",
    )[1]

    tclf: xr.DataArray = _load_zarr(
        tree_cover_loss_from_fires_uri, group="pipeline"
    ).band_data
    tclf = xr.align(
        tcl,
        tclf.reindex_like(tcl, method="nearest", tolerance=1e-5, fill_value=0),
        join="left",
    )[1]
    # Convert tclf to a non-zero pixel area where there is a loss year, so it can be
    # used as part of the mask. The value of the loss year doesn't matter, since it
    # is always the same as the TCL loss year.
    tclf_area = (tclf > 0) * pixel_area

    mangrove_stock_2000: xr.DataArray = _load_zarr(mangrove_stock_2000_uri).band_data
    mangrove_stock_2000 = xr.align(
        tcl,
        mangrove_stock_2000.reindex_like(
            tcl, method="nearest", tolerance=1e-5, fill_value=0
        ),
        join="left",
    )[1].astype(np.uint8)

    tree_cover_gain_from_height: xr.DataArray = _load_zarr(
        tree_cover_gain_from_height_uri
    ).band_data
    tree_cover_gain_from_height = xr.align(
        tcl,
        tree_cover_gain_from_height.reindex_like(
            tcl, method="nearest", tolerance=1e-5, fill_value=0
        ),
        join="left",
    )[1]
    tree_cover_gain_from_height = xr.where(
        tree_cover_gain_from_height > 0, 1, 0
    ).astype(np.uint8)

    # Apply additional carbon filters for mangrove and tree-cover-gain pixels.
    carbon_emissions_filtered = (
        (mangrove_stock_2000 == 1) | (tree_cover_gain_from_height == 1)
    ) * carbon_emissions

    # contextual layers
    tcd: xr.DataArray = _load_zarr(tree_cover_density_uri).band_data
    tcd = xr.align(
        tcl, tcd.reindex_like(tcl, method="nearest", tolerance=1e-5), join="left"
    )[1]

    ifl: xr.DataArray = _load_zarr(ifl_uri).band_data
    ifl = xr.align(
        tcl,
        ifl.reindex_like(tcl, method="nearest", tolerance=1e-5, fill_value=0),
        join="left",
    )[1].astype(np.int16)

    drivers: xr.DataArray = _load_zarr(drivers_uri, group=group).band_data
    drivers = xr.align(
        tcl,
        drivers.reindex_like(tcl, method="nearest", tolerance=1e-5, fill_value=0),
        join="left",
    )[1].astype(np.int16)

    primary_forests: xr.DataArray = _load_zarr(primary_forests_uri).band_data
    primary_forests = xr.align(
        tcl,
        primary_forests.reindex_like(
            tcl, method="nearest", tolerance=1e-5, fill_value=0
        ),
        join="left",
    )[1]

    natural_forests: xr.DataArray = _load_zarr(natural_forests_uri).band_data
    natural_forests = xr.align(
        tcl,
        natural_forests.reindex_like(
            tcl, method="nearest", tolerance=1e-5, fill_value=0
        ),
        join="left",
    )[1]

    # GADM zarrs
    country: xr.DataArray = _load_zarr(country_zarr_uri).band_data
    country = xr.align(
        tcl,
        country.reindex_like(tcl, method="nearest", tolerance=1e-5),
        join="left",
    )[1].astype(np.int16)

    region: xr.DataArray = _load_zarr(region_zarr_uri).band_data
    region = xr.align(
        tcl, region.reindex_like(tcl, method="nearest", tolerance=1e-5), join="left"
    )[1].astype(np.uint8)

    subregion: xr.DataArray = _load_zarr(subregion_zarr_uri).band_data
    subregion = xr.align(
        tcl,
        subregion.reindex_like(tcl, method="nearest", tolerance=1e-5),
        join="left",
    )[1].astype(np.int16)

    # combine area with emissions to sum both together
    area_and_emissions = xr.Dataset(
        {
            "area_ha": pixel_area,
            "carbon__Mg_CO2e": carbon_emissions_filtered,
            "tree_cover_loss_from_fires_area_ha": tclf_area,
        }
    )

    return (
        tcl,
        area_and_emissions,
        tcd,
        ifl,
        drivers,
        primary_forests,
        natural_forests,
        country,
        region,
        subregion,
    )


def setup_compute(
    datasets: Tuple,
    expected_groups,
) -> Tuple:
    """Setup the arguments for the xarray reduce on tree cover loss by area and emissions"""
    (
        tcl,
        area_and_emissions,
        tcd,
        ifl,
        drivers,
        primary_forests,
        natural_forests,
        country,
        region,
        subregion,
    ) = datasets

    mask = xr.concat(
        [
            area_and_emissions["area_ha"],
            area_and_emissions["carbon__Mg_CO2e"],
            area_and_emissions["tree_cover_loss_from_fires_area_ha"],
        ],
        pd.Index(
            ["area_ha", "carbon_Mg_CO2e", "tree_cover_loss_from_fires_area_ha"],
            name="layer",
        ),
    )

    groupbys: Tuple[xr.DataArray, ...] = (
        tcl.rename("tree_cover_loss_year"),
        tcd.rename("canopy_cover"),
        ifl.rename("is_intact_forest"),
        drivers.rename("driver"),
        primary_forests.rename("is_primary_forest"),
        natural_forests.rename("natural_forest_class"),
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )

    return (mask, groupbys, expected_groups)


def create_result_dataframe(result: xr.DataArray) -> pd.DataFrame:
    """
    Convert an xarray with multiple layers to a result df
    """
    # extract sparse data
    sparse_data = result.data
    dim_names = result.dims
    indices = sparse_data.coords
    values = sparse_data.data

    # create coordinate dictionary
    coord_dict = {
        dim: result.coords[dim].values[indices[i]] for i, dim in enumerate(dim_names)
    }
    coord_dict["value"] = values

    df = pd.DataFrame(coord_dict)

    # pivot to get separate cols for each layer
    df_pivoted = df.pivot_table(
        index=[col for col in df.columns if col not in ["layer", "value"]],
        columns="layer",
        values="value",
        fill_value=0,
    ).reset_index()
    df_pivoted.columns.name = None

    return df_pivoted


def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
    result_df = create_result_dataframe(result)
    # convert year values (1-24) to actual years (2001-2024)

    result_df["tree_cover_loss_year"] = result_df["tree_cover_loss_year"] + 2000

    # convert tcl thresholds to percentages
    result_df["canopy_cover"] = result_df["canopy_cover"].map(thresh_to_pct)

    # convert ifl to boolean
    result_df["is_intact_forest"] = result_df["is_intact_forest"].astype(bool)

    # convert driver codes to labels
    categoryid_to_driver = {
        0: "Unknown",
        1: "Permanent agriculture",
        2: "Hard commodities",
        3: "Shifting cultivation",
        4: "Logging",
        5: "Wildfire",
        6: "Settlements and infrastructure",
        7: "Other natural disturbances",
    }

    result_df["driver"] = result_df["driver"].map(categoryid_to_driver)

    # convert primary forest to boolean
    result_df["is_primary_forest"] = result_df["is_primary_forest"].astype(bool)

    natural_forest_class_to_label = {
        0: "Unknown",
        1: "Natural Forest",
        2: "Non-natural Forest",
    }
    result_df["natural_forest_class"] = result_df["natural_forest_class"].map(
        natural_forest_class_to_label
    )

    result_df["country"] = result_df["country"].map(numeric_to_alpha3)
    result_df.dropna(subset=["country"], inplace=True)

    results_with_ids = rollup_by_gadm_and_convert_to_aoi(
        result_df,
        [
            "tree_cover_loss_year",
            "canopy_cover",
            "is_intact_forest",
            "driver",
            "is_primary_forest",
            "natural_forest_class",
        ],
    )

    return results_with_ids


def _symmetric_relative_difference(a, b):
    avg = (abs(a) + abs(b)) / 2
    return 0 if avg == 0 else abs(a - b) / avg


# In Justin's original QC feature file (now at
# s3://lcl-analytics/vectors/qc_features.geojson.orig), only 3 features in the
# first 55 did not pass. Those GADM2 areas are listed in bug GTC-3496 to
# investigate why they are off by 3-4%. Those 3 features were removed from the QC
# feature file, along with 8 other features that took 6 or more minutes for the
# GEE processing to run. The result is the first 44 feature should pass (and we
# are checking the first 20 by default).
def qc_against_validation_source(
    result_df: pd.DataFrame,
    version: Optional[str] = None,
    qc_feature_repository=None,
    gee_repository=None,
    qc_error_threshold=0.01,
):
    if qc_feature_repository is None:
        qc_feature_repository = QCFeaturesRepository()
    if gee_repository is None:
        gee_repository = GoogleEarthEngineDatasetRepository()

    qc_features = qc_feature_repository.load(limit=20)

    def qc_feature(row):
        print(f"Starting QC on GID {row.GID_2}")
        admin2_aoi_id = row.GID_2.split("_")[0]
        sample_stats = result_df[result_df.aoi_id == admin2_aoi_id]

        if sample_stats.size > 0:
            sample_driver_area_ha_total = sample_stats[
                (sample_stats.canopy_cover.astype(np.int8) >= 30)
                & (sample_stats.driver != "Unknown")
            ].area_ha.sum()

            sample_natural_forests_ha_total = sample_stats[
                (sample_stats.natural_forest_class != "Unknown")
                & (sample_stats.tree_cover_loss_year > 2020)
            ].area_ha.sum()
        else:
            sample_driver_area_ha_total = 0
            sample_natural_forests_ha_total = 0

        validation_stats = get_validation_statistics(row.geometry, gee_repository)
        if validation_stats["driver_results"].size > 0:
            validation_driver_area_ha_total = validation_stats[
                "driver_results"
            ].area_ha.sum()
        else:
            validation_driver_area_ha_total = 0

        if validation_stats["natural_forests_results"].size > 0:
            validation_natural_forests_ha_total = validation_stats[
                "natural_forests_results"
            ].area_ha.sum()
        else:
            validation_natural_forests_ha_total = 0

        diff_driver = _symmetric_relative_difference(
            validation_driver_area_ha_total, sample_driver_area_ha_total
        )
        diff_natural_forest = _symmetric_relative_difference(
            validation_natural_forests_ha_total, sample_natural_forests_ha_total
        )

        driver_result = bool(diff_driver < qc_error_threshold)
        natural_forest_result = bool(diff_natural_forest < qc_error_threshold)

        r = pd.Series(
            {
                "pass": all([driver_result, natural_forest_result]),
                "sample_driver": sample_driver_area_ha_total,
                "validation_driver": validation_driver_area_ha_total,
                "sample_natural_forest": sample_natural_forests_ha_total,
                "validation_natural_forest": validation_natural_forests_ha_total,
                "detail": "",
            }
        )
        print(f"\n{row.GID_2}\n{r}")
        return r

    qc_features[
        [
            "qc_pass",
            "sample_driver",
            "validation_driver",
            "sample_natural_forest",
            "validation_natural_forest",
            "detail",
        ]
    ] = qc_features.apply(qc_feature, axis=1)

    if version is not None:
        qc_feature_repository.write_results(
            qc_features, "admin-tree-cover-loss", version
        )
    return bool(qc_features.qc_pass.all())


def get_validation_statistics(
    geom: Polygon,
    gee_repository=None,
) -> Dict[str, pd.DataFrame]:
    if gee_repository is None:
        gee_repository = GoogleEarthEngineDatasetRepository()

    loss_ds = gee_repository.load("loss", geom)

    # pull only what we need
    loss = loss_ds.loss  # 0/1
    tcd = loss_ds.treecover2000  # 0-100
    loss_year = loss_ds.lossyear

    loss_mask = loss == 1
    loss_tcd30_mask = loss_mask & (tcd > 30)

    drivers_ds = gee_repository.load("tcl_drivers", geom, like=loss)
    drivers_class = drivers_ds.classification.where(loss_tcd30_mask)

    natural_lands_class = gee_repository.load(
        "natural_lands", geom, like=loss
    ).classification
    natural_forests_class = xr.where(
        natural_lands_class.isin([2, 5, 8, 9]),
        1,
        xr.where(natural_lands_class.isin([14, 17, 18]), 2, 0),
    )

    area = gee_repository.load("area", geom, like=loss) / 10000

    # if the whole thing is masked just exit early
    if loss_tcd30_mask.isnull().all().item() or drivers_class.isnull().all().item():
        driver_results = pd.DataFrame({"area_ha": [], "driver": []})
    else:
        driver_results = (
            area.groupby(drivers_class).sum(skipna=True).to_dataframe().reset_index()
        )
        driver_results = driver_results.rename(
            columns={"area": "area_ha", "classification": "driver"}
        )

    natural_forests_results = (
        area.where(loss_year > 20)
        .where(natural_forests_class > 0)
        .groupby(natural_forests_class)
        .sum()
        .to_dataframe()
        .reset_index()
    )
    natural_forests_results = natural_forests_results.rename(
        columns={"area": "area_ha", "classification": "natural_forests_class"}
    )

    return {
        "driver_results": driver_results,
        "natural_forests_results": natural_forests_results,
    }
