from typing import Tuple, Optional
import pandas as pd
import xarray as xr
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)

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

def load_data(
    dist_zarr_uri: str,
    contextual_uri: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the Dist alert Zarr, the GADM zarrs, and possibly a contextual layer zarr"""

    dist_alerts = _load_zarr(dist_zarr_uri)

    # reindex to dist alerts to avoid floating point precision issues
    # when aligning the datasets
    # https://github.com/pydata/xarray/issues/2217
    country = _load_zarr(country_zarr_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    country_aligned = xr.align(dist_alerts, country, join="left")[1].band_data
    region = _load_zarr(region_zarr_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    region_aligned = xr.align(dist_alerts, region, join="left")[1].band_data
    subregion = _load_zarr(subregion_zarr_uri).reindex_like(
        dist_alerts, method="nearest", tolerance=1e-5
    )
    subregion_aligned = xr.align(dist_alerts, subregion, join="left")[1].band_data

    if contextual_uri is not None:
        contextual_layer = _load_zarr(contextual_uri).reindex_like(
            dist_alerts, method="nearest", tolerance=1e-5
        )
        contextual_layer_aligned = xr.align(dist_alerts, contextual_layer, join="left")[
            1
        ].band_data
    else:
        contextual_layer_aligned = None

    return (
        dist_alerts,
        country_aligned,
        region_aligned,
        subregion_aligned,
        contextual_layer_aligned,
    )

def compute(reduce_mask: xr.DataArray, reduce_groupbys: Tuple, expected_groups: Tuple, funcname: str) -> xr.DataArray:
    print("Starting reduce")
    alerts_count = xarray_reduce(
        reduce_mask,
        *reduce_groupbys,
        func=funcname,
        expected_groups=expected_groups,
        reindex=ReindexStrategy(
            blockwise=False, array_type=ReindexArrayType.SPARSE_COO
        ),
        fill_value=0,
    ).compute()
    print("Finished reduce")
    return alerts_count


def create_result_dataframe(alerts_count: xr.DataArray) -> pd.DataFrame:
    sparse_data = alerts_count.data
    dim_names = alerts_count.dims
    indices = sparse_data.coords
    values = sparse_data.data

    coord_dict = {
        dim: alerts_count.coords[dim].values[idx]
        for dim, idx in zip(dim_names, indices)
    }
    coord_dict["value"] = values
    df = pd.DataFrame(coord_dict)
    df["country"] = df["country"].apply(lambda x: numeric_to_alpha3.get(x, None))
    df.dropna(subset="country", inplace=True)

    return df


def save_results(
    df: pd.DataFrame, results_uri: str
) -> str:
    print("Starting parquet")

    _save_parquet(df, results_uri)
    print("Finished parquet")
    return results_uri


# _load_zarr and _save_parquet are the functions being mocked by the unit tests.
def _save_parquet(df: pd.DataFrame, results_uri: str) -> None:
    df.to_parquet(results_uri, index=False)


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri)
