from typing import Callable, Dict, Optional, Tuple

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
    create_result_dataframe as common_create_result_dataframe,
)
from pipelines.prefect_flows.common_stages import create_zarrs as common_create_zarrs
from pipelines.prefect_flows.common_stages import (
    rollup_by_gadm_and_convert_to_aoi,
    symmetric_relative_difference,
)
from pipelines.repositories.google_earth_engine_dataset_repository import (
    GoogleEarthEngineDatasetRepository,
)
from pipelines.repositories.qc_feature_repository import QCFeaturesRepository

PIPELINE_CHUNK_SIZE = 10_000
OTF_CHUNK_SIZE = 4_000

CARBON_NET_FLUX_VERSION = "v20250430"
CARBON_GROSS_REMOVALS_VERSION = "v20250416"
CARBON_GROSS_EMISSIONS_VERSION = "v20250430"

DATASETS = {
    "carbon_net_flux": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/gfw_forest_carbon_net_flux/{CARBON_NET_FLUX_VERSION}/raster/epsg-4326/10/40000/Mg_CO2e_px-1/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/gfw-carbon-net-flux/{CARBON_NET_FLUX_VERSION}/Mg_CO2e.zarr",
        "dtype": "float64",
    },
    "carbon_gross_removals": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/gfw_forest_carbon_gross_removals/{CARBON_GROSS_REMOVALS_VERSION}/raster/epsg-4326/10/40000/Mg_CO2e_px-1/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/gfw-carbon-gross-removals/{CARBON_GROSS_REMOVALS_VERSION}/Mg_CO2e.zarr",
        "dtype": "float64",
    },
    "carbon_gross_emissions": {
        "tiles_uri": f"s3://{DATA_LAKE_BUCKET}/gfw_forest_carbon_gross_emissions/{CARBON_GROSS_EMISSIONS_VERSION}/raster/epsg-4326/10/40000/Mg_CO2e_px-1/gdal-geotiff/tiles.geojson",
        "zarr_uri": f"s3://{ANALYTICS_BUCKET}/zarr/gfw-carbon-gross-emissions/{CARBON_GROSS_EMISSIONS_VERSION}/Mg_CO2e.zarr",
        "dtype": "float64",
    },
}

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]


def create_zarrs(overwrite: bool = False) -> Dict[str, str]:
    """Create zarr stores for carbon flux datasets from tiled GeoTIFFs."""
    return common_create_zarrs(
        datasets=DATASETS,
        overwrite=overwrite,
        pipeline_chunk_size=PIPELINE_CHUNK_SIZE,
        otf_chunk_size=OTF_CHUNK_SIZE,
    )


def load_data(
    carbon_net_flux_zarr_uri,
    carbon_gross_removals_zarr_uri,
    carbon_gross_emissions_zarr_uri,
    tree_cover_density_2000_zarr_uri,
    mangrove_stock_2000_zarr_uri,
    tree_cover_gain_from_height_zarr_uri,
    group: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the layers needed for carbon flux analysis"""

    base_layer = _load_zarr(carbon_net_flux_zarr_uri, group=group).astype("float64")
    carbon_gross_removals = (
        _load_zarr(carbon_gross_removals_zarr_uri, group=group)
        .reindex_like(base_layer, method="nearest", tolerance=1e-5)
        .astype("float64")
    )
    carbon_gross_emissions = (
        _load_zarr(carbon_gross_emissions_zarr_uri, group=group)
        .reindex_like(base_layer, method="nearest", tolerance=1e-5)
        .astype("float64")
    )

    # reindex to dist alerts to avoid floating point precision issues
    # when aligning the datasets
    # https://github.com/pydata/xarray/issues/2217
    country = _load_zarr(country_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    country_aligned = xr.align(base_layer, country, join="left")[1].band_data
    region = _load_zarr(region_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    region_aligned = xr.align(base_layer, region, join="left")[1].band_data
    subregion = _load_zarr(subregion_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    subregion_aligned = xr.align(base_layer, subregion, join="left")[1].band_data

    # Very important "fill_value = 0" argument here, since mangroves has lots of
    # non-existent tiles, so the zarr file has a much smaller extent than the other
    # global datasets (including the base carbon layer). This effectively means those
    # areas will be filled with NoData/Nans, which will mess up the groupby. So, we
    # make sure to fill all those empty areas with 0 during the reindex to the base
    # layer.
    mangrove_stock_2000 = _load_zarr(mangrove_stock_2000_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5, fill_value=0
    )
    mangrove_stock_2000_aligned = xr.align(
        base_layer, mangrove_stock_2000, join="left"
    )[1].band_data

    tree_cover_gain_from_height = _load_zarr(
        tree_cover_gain_from_height_zarr_uri
    ).reindex_like(base_layer, method="nearest", tolerance=1e-5)
    tree_cover_gain_from_height_aligned = xr.align(
        base_layer, tree_cover_gain_from_height, join="left"
    )[1].band_data
    # Map tree_cover_gain_from_height to 1 if any gain, 0 otherwise.
    tree_cover_gain_from_height_aligned = xr.where(
        tree_cover_gain_from_height_aligned > 0, 1, 0
    ).astype("uint8")

    tree_cover_density_2000 = _load_zarr(tree_cover_density_2000_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    tree_cover_density_2000_aligned = xr.align(
        base_layer, tree_cover_density_2000, join="left"
    )[1].band_data
    # Only keep the 30/50/75 densities (codes 5, 6, and 7), map the rest (10, 15, 20,
    # 25) to 0, which means less than 30% density.
    tree_cover_density_2000_aligned = tree_cover_density_2000_aligned.where(
        tree_cover_density_2000_aligned >= 5, other=0
    )

    return (
        base_layer,
        carbon_gross_removals,
        carbon_gross_emissions,
        country_aligned,
        region_aligned,
        subregion_aligned,
        mangrove_stock_2000_aligned,
        tree_cover_gain_from_height_aligned,
        tree_cover_density_2000_aligned,
    )


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xrarray reduce on natural lands by area"""
    (
        carbon_net_flux,
        carbon_gross_removals,
        carbon_gross_emissions,
        country,
        region,
        subregion,
        mangrove_stock_2000,
        tree_cover_gain_from_height,
        tree_cover_density_2000,
    ) = datasets

    # I created a data array with the multiple data inputs on a "carbontype"
    # dimension, rather than a dataset with multiple data variables, because only a
    # data array result works with the create_result_dataframe() post-processing of the
    # sparse data.
    ds = xr.concat(
        [
            carbon_net_flux.band_data,
            carbon_gross_removals.band_data,
            carbon_gross_emissions.band_data,
        ],
        pd.Index(
            ["carbon_net_flux", "carbon_gross_removals", "carbon_gross_emissions"],
            name="carbontype",
        ),
    )

    groupbys: Tuple[xr.DataArray, ...] = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        tree_cover_density_2000.rename("tree_cover_density"),
        mangrove_stock_2000.rename("mangrove_stock_2000"),
        tree_cover_gain_from_height.rename("tree_cover_gain_from_height"),
    )

    return (ds, groupbys, expected_groups)


# tcd threshold mapping
thresh_to_pct = {
    0: 0,
    1: 10,
    2: 15,
    3: 20,
    4: 25,
    5: 30,
    6: 50,
    7: 75,
}


def create_result_dataframe(alerts_count: xr.DataArray) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_count)

    # Convert tcd thresholds to percentages
    df["tree_cover_density"] = df["tree_cover_density"].map(thresh_to_pct)

    # For each tcd value T in [30, 50, 75], aggregate carbon emissions/removal/flux if
    # (tree_cover_density >= T) OR (mangrove_stock_2000 is True) OR (tree_cover_gain_from_height is non-zero)
    thresholds = [30, 50, 75]
    results = []

    for T in thresholds:
        mask = (
            (df["tree_cover_density"] >= T)
            | (df["mangrove_stock_2000"] == 1)
            | (df["tree_cover_gain_from_height"] == 1)
        )

        temp_df = (
            df[mask]
            .groupby(["country", "region", "subregion", "carbontype"], as_index=False)[
                "value"
            ]
            .sum()
        )
        temp_df["tree_cover_density"] = T
        results.append(temp_df)

    df = pd.concat(results, ignore_index=True)
    df = df[
        ["country", "region", "subregion", "tree_cover_density", "carbontype", "value"]
    ]

    df = rollup_by_gadm_and_convert_to_aoi(df, ["tree_cover_density", "carbontype"])

    return df


def _load_zarr(zarr_uri, group=None):
    return xr.open_zarr(zarr_uri, group=group)


def qc_against_validation_source(
    result_df: pd.DataFrame,
    qc_feature_repository=None,
    gee_repository=None,
    qc_error_threshold: float = 0.02,
) -> bool:
    if qc_feature_repository is None:
        qc_feature_repository = QCFeaturesRepository()
    if gee_repository is None:
        gee_repository = GoogleEarthEngineDatasetRepository()

    qc_features = qc_feature_repository.load(limit=20)

    def qc_feature(row):
        print(f"Starting QC on GID {row.GID_2}")
        admin2_aoi_id = row.GID_2.split("_")[0]
        sample = result_df[
            (result_df.aoi_id == admin2_aoi_id) & (result_df.tree_cover_density == 30)
        ]

        if sample.size > 0:
            sample_emissions = sample[
                sample.carbontype == "carbon_gross_emissions"
            ].value.sum()
            sample_removals = sample[
                sample.carbontype == "carbon_gross_removals"
            ].value.sum()
        else:
            sample_emissions = 0
            sample_removals = 0

        validation = get_carbon_validation_statistics(row.geometry, gee_repository)

        diff_emissions = symmetric_relative_difference(
            validation["emissions_Mg_CO2e"], sample_emissions
        )
        diff_removals = symmetric_relative_difference(
            validation["removals_Mg_CO2e"], sample_removals
        )

        r = pd.Series(
            {
                "pass": bool(
                    diff_emissions < qc_error_threshold
                    and diff_removals < qc_error_threshold
                ),
                "sample_emissions": sample_emissions,
                "validation_emissions": validation["emissions_Mg_CO2e"],
                "sample_removals": sample_removals,
                "validation_removals": validation["removals_Mg_CO2e"],
            }
        )
        print(f"\n{row.GID_2}\n{r}")
        return r

    qc_features[
        [
            "qc_pass",
            "sample_emissions",
            "validation_emissions",
            "sample_removals",
            "validation_removals",
        ]
    ] = qc_features.apply(qc_feature, axis=1)

    return bool(qc_features.qc_pass.all())


def get_carbon_validation_statistics(
    geom: Polygon,
    gee_repository=None,
) -> Dict[str, float]:
    if gee_repository is None:
        gee_repository = GoogleEarthEngineDatasetRepository()

    emissions = gee_repository.load("carbon_gross_emissions", geom)
    removals = gee_repository.load("carbon_gross_removals", geom)

    area_ha = gee_repository.load("area", geom, like=emissions.b1)["area"] / 10000

    emissions_total = float((emissions.b1 * area_ha).sum(skipna=True).item())
    removals_total = float((removals.b1 * area_ha).sum(skipna=True).item())

    return {
        "emissions_Mg_CO2e": emissions_total,
        "removals_Mg_CO2e": removals_total,
    }

