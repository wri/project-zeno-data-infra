import pandas as pd
import geopandas as gpd
import pandera.pandas as pa
from pandera.typing.pandas import Series
from typing import List
from prefect import task
from prefect.logging import get_run_logger
import rasterio as rio
from rasterio.windows import from_bounds
from rasterio.features import geometry_mask
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import date

from pipelines.disturbance.check_for_new_alerts import get_latest_version

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

numeric_to_alpha3 = {
    4: 'AFG', 248: 'ALA', 8: 'ALB', 12: 'DZA', 16: 'ASM', 20: 'AND', 24: 'AGO', 660: 'AIA',
    10: 'ATA', 28: 'ATG', 32: 'ARG', 51: 'ARM', 533: 'ABW', 36: 'AUS', 40: 'AUT', 31: 'AZE',
    44: 'BHS', 48: 'BHR', 50: 'BGD', 52: 'BRB', 112: 'BLR', 56: 'BEL', 84: 'BLZ', 204: 'BEN',
    60: 'BMU', 64: 'BTN', 68: 'BOL', 535: 'BES', 70: 'BIH', 72: 'BWA', 74: 'BVT', 76: 'BRA',
    86: 'IOT', 96: 'BRN', 100: 'BGR', 854: 'BFA', 108: 'BDI', 132: 'CPV', 116: 'KHM', 120: 'CMR',
    124: 'CAN', 136: 'CYM', 140: 'CAF', 148: 'TCD', 152: 'CHL', 156: 'CHN', 162: 'CXR', 166: 'CCK',
    170: 'COL', 174: 'COM', 178: 'COG', 180: 'COD', 184: 'COK', 188: 'CRI', 384: 'CIV', 191: 'HRV',
    192: 'CUB', 531: 'CUW', 196: 'CYP', 203: 'CZE', 208: 'DNK', 262: 'DJI', 212: 'DMA', 214: 'DOM',
    218: 'ECU', 818: 'EGY', 222: 'SLV', 226: 'GNQ', 232: 'ERI', 233: 'EST', 748: 'SWZ', 231: 'ETH',
    238: 'FLK', 234: 'FRO', 242: 'FJI', 246: 'FIN', 250: 'FRA', 254: 'GUF', 258: 'PYF', 260: 'ATF',
    266: 'GAB', 270: 'GMB', 268: 'GEO', 276: 'DEU', 288: 'GHA', 292: 'GIB', 300: 'GRC', 304: 'GRL',
    308: 'GRD', 312: 'GLP', 316: 'GUM', 320: 'GTM', 831: 'GGY', 324: 'GIN', 624: 'GNB', 328: 'GUY',
    332: 'HTI', 334: 'HMD', 336: 'VAT', 340: 'HND', 344: 'HKG', 348: 'HUN', 352: 'ISL', 356: 'IND',
    360: 'IDN', 364: 'IRN', 368: 'IRQ', 372: 'IRL', 833: 'IMN', 376: 'ISR', 380: 'ITA', 388: 'JAM',
    392: 'JPN', 832: 'JEY', 400: 'JOR', 398: 'KAZ', 404: 'KEN', 296: 'KIR', 408: 'PRK', 410: 'KOR',
    414: 'KWT', 417: 'KGZ', 418: 'LAO', 428: 'LVA', 422: 'LBN', 426: 'LSO', 430: 'LBR', 434: 'LBY',
    438: 'LIE', 440: 'LTU', 442: 'LUX', 446: 'MAC', 450: 'MDG', 454: 'MWI', 458: 'MYS', 462: 'MDV',
    466: 'MLI', 470: 'MLT', 584: 'MHL', 474: 'MTQ', 478: 'MRT', 480: 'MUS', 175: 'MYT', 484: 'MEX',
    583: 'FSM', 498: 'MDA', 492: 'MCO', 496: 'MNG', 499: 'MNE', 500: 'MSR', 504: 'MAR', 508: 'MOZ',
    104: 'MMR', 516: 'NAM', 520: 'NRU', 524: 'NPL', 528: 'NLD', 540: 'NCL', 554: 'NZL', 558: 'NIC',
    562: 'NER', 566: 'NGA', 570: 'NIU', 574: 'NFK', 807: 'MKD', 580: 'MNP', 578: 'NOR', 512: 'OMN',
    586: 'PAK', 585: 'PLW', 275: 'PSE', 591: 'PAN', 598: 'PNG', 600: 'PRY', 604: 'PER', 608: 'PHL',
    612: 'PCN', 616: 'POL', 620: 'PRT', 630: 'PRI', 634: 'QAT', 638: 'REU', 642: 'ROU', 643: 'RUS',
    646: 'RWA', 652: 'BLM', 654: 'SHN', 659: 'KNA', 662: 'LCA', 663: 'MAF', 666: 'SPM', 670: 'VCT',
    882: 'WSM', 674: 'SMR', 678: 'STP', 682: 'SAU', 686: 'SEN', 688: 'SRB', 690: 'SYC', 694: 'SLE',
    702: 'SGP', 534: 'SXM', 703: 'SVK', 705: 'SVN', 90: 'SLB', 706: 'SOM', 710: 'ZAF', 239: 'SGS',
    728: 'SSD', 724: 'ESP', 144: 'LKA', 729: 'SDN', 740: 'SUR', 744: 'SJM', 752: 'SWE', 756: 'CHE',
    760: 'SYR', 158: 'TWN', 762: 'TJK', 834: 'TZA', 764: 'THA', 626: 'TLS', 768: 'TGO', 772: 'TKL',
    776: 'TON', 780: 'TTO', 788: 'TUN', 792: 'TUR', 795: 'TKM', 796: 'TCA', 798: 'TUV', 800: 'UGA',
    804: 'UKR', 784: 'ARE', 826: 'GBR', 840: 'USA', 581: 'UMI', 858: 'URY', 860: 'UZB', 548: 'VUT',
    862: 'VEN', 704: 'VNM', 92: 'VGB', 850: 'VIR', 876: 'WLF', 732: 'ESH', 887: 'YEM', 894: 'ZMB',
    716: 'ZWE'
}

sbtn_natural_lands_classes = [
    "Forest", "Short vegetation", "Water", "Mangroves", "Bare", "Snow/Ice", "Wetland forest", "Peat forest",
    "Wetland short vegetation", "Peat short vegetation", "Cropland", "Built-up", "Tree cover", "Short vegetation",
    "Water", "Wetland tree cover", "Peat tree cover", "Wetland short vegetation", "Peat short vegetation", "Bare"
]

class DistZonalStats(pa.DataFrameModel):
    country: Series[str] = pa.Field(eq="BRA")
    region: Series[int] = pa.Field(eq=20) # gadm id for adm1 AOI
    subregion: Series[int] = pa.Field(lt=170) # placeholder adm2
    alert_date: Series[date] = pa.Field(ge=date.fromisoformat("2023-01-01"), le=date.fromisoformat("2025-06-28")) # julian date between 2023-01-01 to latest version
    alert_confidence: Series[str] = pa.Field(isin=["low", "high"]) # low confidence, high confidence
    area: Series[float]

    class Config:
        coerce = True
        strict = True
        name = "ZonalStatsSchema"
        ordered = True
        unique = ["country", "region", "subregion", "alert_date", "alert_confidence"]

    @staticmethod
    def calculate_area_sums_by_confidence(df: pd.DataFrame) -> dict:
        """Calculate the total area by confidence level."""
        area_sums = df.groupby("alert_confidence")["area__ha"].sum().to_dict()
        return {
            "low_confidence": area_sums.get("low", 0),
            "high_confidence": area_sums.get("high", 0),
        }

    @staticmethod
    def spot_check_julian_dates(df: pd.DataFrame, julian_dates: List[date]) -> pd.DataFrame:
        filtered_by_date_df = df[df["alert_date"].isin(julian_dates)]
        filtered_by_date_df = filtered_by_date_df.sort_values(by="alert_date").reset_index(drop=True)
        return filtered_by_date_df[["alert_date", "alert_confidence", "area__ha"]]

class NaturalLandsZonalStats(pa.DataFrameModel):
    countries: Series[str] = pa.Field(isin=isos)
    regions: Series[int] = pa.Field()
    subregions: Series[int] = pa.Field()
    natural_lands: Series[str] = pa.Field(isin=sbtn_natural_lands_classes)
    area: Series[float] = pa.Field(ge=0)

def generate_validation_statistics(version: str) -> pd.DataFrame:
    """Generate zonal statistics for the admin area AOI."""
    gdf = gpd.read_file("validation_statistics/br_rn.json") # State of Rio Grande do Norte, Brazil
    aoi = gdf.iloc[0]
    aoi_tile = "00N_040W" # This AOI fits within a tile, but we should build VRTs so we can use any (resonably sized) AOI

    # read dist alerts for AOI
    bounds = aoi.geometry.bounds
    with rio.open(f"s3://gfw-data-lake/umd_glad_dist_alerts/{version}/raster/epsg-4326/10/40000/default/gdal-geotiff/{aoi_tile}.tif") as src:
        window = from_bounds(
            bounds[0],
            bounds[1],
            bounds[2],
            bounds[3],
            src.transform
        )
        dist_alerts = src.read(1, window=window)
        win_affine = src.window_transform(window)

    # read area for AOI
    with rio.open(f"s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/10/40000/default/gdal-geotiff/{aoi_tile}.tif") as src:
        pixel_area__m = src.read(1, window=window)
        pixel_area__ha = pixel_area__m / 10000

    # Extract confidence level (first digit)
    dist_confidence_levels = dist_alerts // 10000
    dist_high_conf = np.where(dist_confidence_levels == 3, 1, 0)
    dist_low_conf = np.where(dist_confidence_levels == 2, 1, 0)

    # Extract Julian date (remaining digits)
    dist_julian_date = dist_alerts % 10000

    # create geometry_mask to mask dist alerts by aoi geometry
    aoi_mask = geometry_mask([aoi.geometry], invert=True, transform=win_affine, out_shape=dist_alerts.shape)

    # confidence level maskings
    # anything outside the AOI becomes zero
    dist_high_conf_aoi = aoi_mask * dist_high_conf * pixel_area__ha
    dist_low_conf_aoi = aoi_mask * dist_low_conf * pixel_area__ha
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
    high_conf_results["confidence"] = "high"
    high_conf_results["country"] = 76
    high_conf_results["region"] = 20
    high_conf_results["subregion"] = 150 # placeholder for subregion (adm2) since we are running on an adm1 AOI
    low_conf_results["confidence"] = "low"
    low_conf_results["country"] = 76
    low_conf_results["region"] = 20
    low_conf_results["subregion"] = 150 # placeholder for subregion (adm2) since we are running on an adm1 AOI

    # rename high_conf to value
    high_conf_results.rename(columns={"high_conf": "area__ha"}, inplace=True)
    low_conf_results.rename(columns={"low_conf": "area__ha"}, inplace=True)

    # reorder columns to country, region, subregion, alert_date, confidence, value
    high_conf_results = high_conf_results[["country", "region", "subregion", "alert_date", "confidence", "area__ha"]]
    low_conf_results = low_conf_results[["country", "region", "subregion", "alert_date", "confidence", "area__ha"]]

    # concatenate confidence dfs into one validation df
    results = pd.concat([high_conf_results, low_conf_results], ignore_index=True)

    # drop rows where alert_date is zero
    results = results[results["alert_date"] != 0]

    results.rename(columns={"confidence": "alert_confidence"}, inplace=True)
    results['alert_date'] = results.sort_values(by='alert_date').alert_date.apply(lambda x: date(2020, 12, 31) + relativedelta(days=x))
    results["country"] = results["country"].apply(lambda x: numeric_to_alpha3.get(x, None))

    return pd.DataFrame(results)

@task
def validate(parquet_uri: str) -> bool:
    """Validate Zarr to confirm there's no issues with the input transformation."""

    logger = get_run_logger()

    # load local results
    version = get_latest_version("umd_glad_dist_alerts")
    logger.info(f"Generating validation stats for version {version}.")
    validation_df = generate_validation_statistics(version)

    # load zeno stats for aoi
    zeno_df = pd.read_parquet(parquet_uri) # assumes parquet refers to latest version
    zeno_aoi_df = zeno_df[(zeno_df["country"] == "BRA") & (zeno_df["region"] == 20)]
    logger.info("Loaded Zeno stats for admin area.")

    # validate zonal stats schema
    try:
        DistZonalStats.validate(zeno_aoi_df)
        logger.info("Zonal stats schema validation passed.")
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        return False

    # validate area sums
    validation_areas = DistZonalStats.calculate_area_sums_by_confidence(validation_df)
    zeno_areas = DistZonalStats.calculate_area_sums_by_confidence(zeno_aoi_df)
    if validation_areas != zeno_areas:
        logger.error(f"Area sums do not match: {validation_areas} != {zeno_areas}")
        return False
    logger.info("Area sums validation passed.")

    # spot check random dates
    validation_dates = [date.fromisoformat(dstr) for dstr in ["2023-03-11", "2023-06-19", "2023-09-27", "2024-01-05", "2024-04-14", "2024-07-23", "2024-10-31", "2025-02-08"]] # example julian dates (800, 900, ..., 1500)
    validation_spot_check = DistZonalStats.spot_check_julian_dates(validation_df, validation_dates)
    zeno_spot_check = DistZonalStats.spot_check_julian_dates(zeno_aoi_df, validation_dates)
    if not validation_spot_check.equals(zeno_spot_check):
        logger.error("Spot check dataframes do not match.")
        return False
    logger.info("Spot check validation passed.")

    return True
