import pandas as pd
import geopandas as gpd
import pandera.pandas as pa
from pandera.typing.pandas import Series
from typing import List
from prefect.logging import get_run_logger
import rasterio as rio
from rasterio.windows import from_bounds
from rasterio.features import geometry_mask
import numpy as np

from ..pipelines.disturbance.check_for_new_alerts import get_latest_version

isos = [
    'AFG', 'ALA', 'ALB', 'DZA', 'ASM', 'AND', 'AGO', 'AIA', 'ATA', 'ATG', 'ARG', 'ARM', 'ABW', 'AUS', 'AUT', 'AZE',
    'BHS', 'BHR', 'BGD', 'BRB', 'BLR', 'BEL', 'BLZ', 'BEN', 'BMU', 'BTN', 'BOL', 'BES', 'BIH', 'BWA', 'BVT', 'BRA',
    'IOT', 'BRN', 'BGR', 'BFA', 'BDI', 'CPV', 'KHM', 'CMR', 'CAN', 'CYM', 'CAF', 'TCD', 'CHL', 'CHN', 'CXR', 'CCK',
    'COL', 'COM', 'COG', 'COD', 'COK', 'CRI', 'CIV', 'HRV', 'CUB', 'CUW', 'CYP', 'CZE', 'DNK', 'DJI', 'DMA', 'DOM',
    'ECU', 'EGY', 'SLV', 'GNQ', 'ERI', 'EST', 'SWZ', 'ETH', 'FLK', 'FRO', 'FJI', 'FIN', 'FRA', 'GUF', 'PYF', 'ATF',
    'GAB', 'GMB', 'GEO', 'DEU', 'GHA', 'GIB', 'GRC', 'GRL', 'GRD', 'GLP', 'GUM', 'GTM', 'GGY', 'GIN', 'GNB', 'GUY',
    'HTI', 'HMD', 'VAT', 'HND', 'HKG', 'HUN', 'ISL', 'IND', 'IDN', 'IRN', 'IRQ', 'IRL', 'IMN', 'ISR', 'ITA', 'JAM',
    'JPN', 'JEY', 'JOR', 'KAZ', 'KEN', 'KIR', 'PRK', 'KOR', 'KWT', 'KGZ', 'LAO', 'LVA', 'LBN', 'LSO', 'LBR', 'LBY',
    'LIE', 'LTU', 'LUX', 'MAC', 'MDG', 'MWI', 'MYS', 'MDV', 'MLI', 'MLT', 'MHL', 'MTQ', 'MRT', 'MUS', 'MYT', 'MEX',
    'FSM', 'MDA', 'MCO', 'MNG', 'MNE', 'MSR', 'MAR', 'MOZ', 'MMR', 'NAM', 'NRU', 'NPL', 'NLD', 'NCL', 'NZL', 'NIC',
    'NER', 'NGA', 'NIU', 'NFK', 'MKD', 'MNP', 'NOR', 'OMN', 'PAK', 'PLW', 'PSE', 'PAN', 'PNG', 'PRY', 'PER', 'PHL',
    'PCN', 'POL', 'PRT', 'PRI', 'QAT', 'REU', 'ROU', 'RUS', 'RWA', 'BLM', 'SHN', 'KNA', 'LCA', 'MAF', 'SPM', 'VCT',
    'WSM', 'SMR', 'STP', 'SAU', 'SEN', 'SRB', 'SYC', 'SLE', 'SGP', 'SXM', 'SVK', 'SVN', 'SLB', 'SOM', 'ZAF', 'SGS',
    'SSD', 'ESP', 'LKA', 'SDN', 'SUR', 'SJM', 'SWE', 'CHE', 'SYR', 'TWN', 'TJK', 'TZA', 'THA', 'TLS', 'TGO', 'TKL',
    'TON', 'TTO', 'TUN', 'TUR', 'TKM', 'TCA', 'TUV', 'UGA', 'UKR', 'ARE', 'GBR', 'USA', 'UMI', 'URY', 'UZB', 'VUT',
    'VEN', 'VNM', 'VGB', 'VIR', 'WLF', 'ESH', 'YEM', 'ZMB', 'ZWE'
]

sbtn_natural_lands_classes = [
    "Forest", "Short vegetation", "Water", "Mangroves", "Bare", "Snow/Ice", "Wetland forest", "Peat forest",
    "Wetland short vegetation", "Peat short vegetation",  "Cropland", "Built-up", "Tree cover", "Short vegetation",
    "Water", "Wetland tree cover", "Peat tree cover", "Wetland short vegetation", "Peat short vegetation", "Bare"
]

class DistZonalStats(pa.DataFrameModel):
    country: Series[int] = pa.Field(eq=76) # gadm id for Brazil
    region: Series[int] = pa.Field(eq=20) # gadm id for adm1 AOI
    subregion: Series[int] = pa.Field(lt=170) # placeholder adm2
    alert_date: Series[int] = pa.Field(ge=731, le=1640) # julian date between 2023-01-01 to latest version
    confidence: Series[int] = pa.Field(ge=2, le=3) # low confidence, high confidence
    value: Series[int]
    
    class Config:
        coerce = True
        strict = True
        name = "ZonalStatsSchema"
        ordered = True
        unique_columns = ["country", "region", "subregion", "alert_date", "confidence"]

    @staticmethod
    def calculate_alert_counts(df: pd.DataFrame) -> dict:
        """Calculate the number of alerts by confidence level."""
        alert_counts = df["confidence"].value_counts().to_dict()
        return {
            "low_confidence": alert_counts.get(2, 0),
            "high_confidence": alert_counts.get(3, 0),
        }
    
    @staticmethod
    def spot_check_julian_dates(df: pd.DataFrame, julian_dates: List[int]) -> pd.DataFrame:
        filtered_by_date_df = df[df["alert_date"].isin(julian_dates)]
        filtered_by_date_df = filtered_by_date_df.sort_values(by="alert_date").reset_index(drop=True)
        return filtered_by_date_df[["alert_date", "confidence", "value"]]
    
class NaturalLandsZonalStats(pa.DataFrameModel):
    countries: Series[str] = pa.Field(isin=isos)
    regions: Series[int] = pa.Field()
    subregions: Series[int] = pa.Field()
    natural_lands: Series[str] = pa.Field(isin=sbtn_natural_lands_classes)
    area: Series[float] = pa.Field(ge=0)

def generate_validation_statistics(version: str) -> pd.DataFrame:
    """Generate zonal statistics for the admin area AOI."""
    gdf = gpd.read_file("../../test/validation_statistics/br_rn.json") # State of Rio Grande do Norte, Brazil
    aoi = gdf.iloc[0]
    aoi_tile = "00N_040W" # This AOI fits within a tile, but we should build VRTs so we can use any (resonably sized) AOI
    
    # read dist alerts for AOI
    bounds = aoi.geometry.bounds
    with rio.open(f"s3://gfw-data-lake/umd_glad_dist_alerts/{version}/raster/epsg-4326/10/40000/currentweek/gdal-geotiff/{aoi_tile}.tif") as src:
        window = from_bounds(
            bounds[0],
            bounds[1],
            bounds[2],
            bounds[3],
            src.transform
        )
        dist_alerts = src.read(1, window=window)
        win_affine = src.window_transform(window)

    # set no_data from -1 to 0
    dist_alerts = np.where(dist_alerts == -1, 0, dist_alerts)

    # Extract confidence level (first digit)
    dist_confidence_levels = dist_alerts // 10000
    dist_high_conf = np.where(dist_confidence_levels == 3, 1, 0)
    dist_low_conf = np.where(dist_confidence_levels == 2, 1, 0)

    # Extract Julian date (remaining digits)
    dist_julian_date = dist_alerts % 10000 

    # mask dist alerts by aoi geometry
    aoi_mask = geometry_mask([aoi.geometry], invert=True, transform=win_affine, out_shape=dist_alerts.shape)

    # confidence level maskings 
    dist_high_conf_aoi = aoi_mask * dist_high_conf
    dist_low_conf_aoi = aoi_mask * dist_low_conf
    dist_julian_date_aoi = aoi_mask * dist_julian_date

    # create a dataframe of analysis results
    high_conf_flat = dist_high_conf_aoi.flatten()
    low_conf_flat = dist_low_conf_aoi.flatten()
    julian_date_flat = dist_julian_date_aoi.flatten()
    df = pd.DataFrame({
        "alert_date": julian_date_flat,
        "high_conf": high_conf_flat,
        "low_conf": low_conf_flat
    })
    high_conf_results = df.groupby("alert_date")["high_conf"].sum().reset_index()
    low_conf_results = df.groupby("alert_date")["low_conf"].sum().reset_index()

    # set confidence levels and GADM IDs
    high_conf_results["confidence"] = 3
    high_conf_results["country"] = 76
    high_conf_results["region"] = 20
    high_conf_results["subregion"] = 150
    low_conf_results["confidence"] = 2
    low_conf_results["country"] = 76
    low_conf_results["region"] = 20
    low_conf_results["subregion"] = 150

    # rename high_conf to value
    high_conf_results.rename(columns={"high_conf": "value"}, inplace=True)
    low_conf_results.rename(columns={"low_conf": "value"}, inplace=True)

    # reorder columns to country, region, subregion, alert_date, confidence, value
    high_conf_results = high_conf_results[["country", "region", "subregion", "alert_date", "confidence", "value"]]
    low_conf_results = low_conf_results[["country", "region", "subregion", "alert_date", "confidence", "value"]]

    # concatenate confidence dfs into one validation df
    results = pd.concat([high_conf_results, low_conf_results], ignore_index=True)

    return pd.DataFrame(results)
    
def validates_zonal_statistics(parquet_uri: str) -> bool:
    """Validate Zarr to confirm there's no issues with the input transformation."""
    
    logger = get_run_logger()

    # load local results
    version = get_latest_version()
    validation_df = generate_validation_statistics(version)
    logger.info(f"Generating validation stats for version {version}.")

    # load zeno stats for aoi
    zeno_df = pd.read_parquet(parquet_uri)
    zeno_aoi_df = zeno_df[(zeno_df["country"] == 76) & (zeno_df["region"] == 20)]
    logger.info("Loaded Zeno stats for admin area.")

    # validate zonal stats schema
    try:
        DistZonalStats.validate(zeno_aoi_df)
        logger.info("Zonal stats schema validation passed.")
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        return False

    # validate alert counts
    validation_alerts = DistZonalStats.calculate_alert_counts(validation_df)
    zeno_alerts = DistZonalStats.calculate_alert_counts(zeno_aoi_df)
    if validation_alerts != zeno_alerts:
        logger.error(f"Alert counts do not match: {validation_alerts} != {zeno_alerts}")
        return False
    logger.info("Alert counts validation passed.")

    # spot check random dates
    validation_dates = np.random.choice(range(800, 1500), size=10, replace=False).tolist()
    validation_spot_check = DistZonalStats.spot_check_julian_dates(validation_df, validation_dates)
    zeno_spot_check = DistZonalStats.spot_check_julian_dates(zeno_aoi_df, validation_dates)
    if not validation_spot_check.equals(zeno_spot_check):
        logger.error("Spot check dataframes do not match.")
        return False
    logger.info("Spot check validation passed.")
    
    return True
