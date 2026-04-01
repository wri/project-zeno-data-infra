from typing import Dict, List, Optional, Tuple

import pandas as pd
import xarray as xr
from flox import ReindexArrayType, ReindexStrategy
from flox.xarray import xarray_reduce

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.utils import s3_uri_exists

numeric_to_alpha3 = {
    4: "AFG",
    248: "ALA",
    8: "ALB",
    12: "DZA",
    16: "ASM",
    20: "AND",
    24: "AGO",
    660: "AIA",
    10: "ATA",
    28: "ATG",
    32: "ARG",
    51: "ARM",
    533: "ABW",
    36: "AUS",
    40: "AUT",
    31: "AZE",
    44: "BHS",
    48: "BHR",
    50: "BGD",
    52: "BRB",
    112: "BLR",
    56: "BEL",
    84: "BLZ",
    204: "BEN",
    60: "BMU",
    64: "BTN",
    68: "BOL",
    535: "BES",
    70: "BIH",
    72: "BWA",
    74: "BVT",
    76: "BRA",
    86: "IOT",
    96: "BRN",
    100: "BGR",
    854: "BFA",
    108: "BDI",
    132: "CPV",
    116: "KHM",
    120: "CMR",
    124: "CAN",
    136: "CYM",
    140: "CAF",
    148: "TCD",
    152: "CHL",
    156: "CHN",
    162: "CXR",
    166: "CCK",
    170: "COL",
    174: "COM",
    178: "COG",
    180: "COD",
    184: "COK",
    188: "CRI",
    384: "CIV",
    191: "HRV",
    192: "CUB",
    531: "CUW",
    196: "CYP",
    203: "CZE",
    208: "DNK",
    262: "DJI",
    212: "DMA",
    214: "DOM",
    218: "ECU",
    818: "EGY",
    222: "SLV",
    226: "GNQ",
    232: "ERI",
    233: "EST",
    748: "SWZ",
    231: "ETH",
    238: "FLK",
    234: "FRO",
    242: "FJI",
    246: "FIN",
    250: "FRA",
    254: "GUF",
    258: "PYF",
    260: "ATF",
    266: "GAB",
    270: "GMB",
    268: "GEO",
    276: "DEU",
    288: "GHA",
    292: "GIB",
    300: "GRC",
    304: "GRL",
    308: "GRD",
    312: "GLP",
    316: "GUM",
    320: "GTM",
    831: "GGY",
    324: "GIN",
    624: "GNB",
    328: "GUY",
    332: "HTI",
    334: "HMD",
    336: "VAT",
    340: "HND",
    344: "HKG",
    348: "HUN",
    352: "ISL",
    356: "IND",
    360: "IDN",
    364: "IRN",
    368: "IRQ",
    372: "IRL",
    833: "IMN",
    376: "ISR",
    380: "ITA",
    388: "JAM",
    392: "JPN",
    832: "JEY",
    400: "JOR",
    398: "KAZ",
    404: "KEN",
    296: "KIR",
    408: "PRK",
    410: "KOR",
    414: "KWT",
    417: "KGZ",
    418: "LAO",
    428: "LVA",
    422: "LBN",
    426: "LSO",
    430: "LBR",
    434: "LBY",
    438: "LIE",
    440: "LTU",
    442: "LUX",
    446: "MAC",
    450: "MDG",
    454: "MWI",
    458: "MYS",
    462: "MDV",
    466: "MLI",
    470: "MLT",
    584: "MHL",
    474: "MTQ",
    478: "MRT",
    480: "MUS",
    175: "MYT",
    484: "MEX",
    583: "FSM",
    498: "MDA",
    492: "MCO",
    496: "MNG",
    499: "MNE",
    500: "MSR",
    504: "MAR",
    508: "MOZ",
    104: "MMR",
    516: "NAM",
    520: "NRU",
    524: "NPL",
    528: "NLD",
    540: "NCL",
    554: "NZL",
    558: "NIC",
    562: "NER",
    566: "NGA",
    570: "NIU",
    574: "NFK",
    807: "MKD",
    580: "MNP",
    578: "NOR",
    512: "OMN",
    586: "PAK",
    585: "PLW",
    275: "PSE",
    591: "PAN",
    598: "PNG",
    600: "PRY",
    604: "PER",
    608: "PHL",
    612: "PCN",
    616: "POL",
    620: "PRT",
    630: "PRI",
    634: "QAT",
    638: "REU",
    642: "ROU",
    643: "RUS",
    646: "RWA",
    652: "BLM",
    654: "SHN",
    659: "KNA",
    662: "LCA",
    663: "MAF",
    666: "SPM",
    670: "VCT",
    882: "WSM",
    674: "SMR",
    678: "STP",
    682: "SAU",
    686: "SEN",
    688: "SRB",
    690: "SYC",
    694: "SLE",
    702: "SGP",
    534: "SXM",
    703: "SVK",
    705: "SVN",
    90: "SLB",
    706: "SOM",
    710: "ZAF",
    239: "SGS",
    728: "SSD",
    724: "ESP",
    144: "LKA",
    729: "SDN",
    740: "SUR",
    744: "SJM",
    752: "SWE",
    756: "CHE",
    760: "SYR",
    158: "TWN",
    762: "TJK",
    834: "TZA",
    764: "THA",
    626: "TLS",
    768: "TGO",
    772: "TKL",
    776: "TON",
    780: "TTO",
    788: "TUN",
    792: "TUR",
    795: "TKM",
    796: "TCA",
    798: "TUV",
    800: "UGA",
    804: "UKR",
    784: "ARE",
    826: "GBR",
    840: "USA",
    581: "UMI",
    858: "URY",
    860: "UZB",
    548: "VUT",
    862: "VEN",
    704: "VNM",
    92: "VGB",
    850: "VIR",
    876: "WLF",
    732: "ESH",
    887: "YEM",
    894: "ZMB",
    716: "ZWE",
}


def load_data(
    base_zarr_uri: str,
    contextual_uri: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the base zarr, the GADM zarrs, and possibly a contextual layer zarr"""

    base_layer = _load_zarr(base_zarr_uri)

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

    if contextual_uri is not None:
        contextual_layer = _load_zarr(contextual_uri).reindex_like(
            base_layer, method="nearest", tolerance=1e-5
        )
        contextual_layer_aligned = xr.align(base_layer, contextual_layer, join="left")[
            1
        ].band_data
    else:
        contextual_layer_aligned = None

    return (
        base_layer,
        country_aligned,
        region_aligned,
        subregion_aligned,
        contextual_layer_aligned,
    )


def compute(
    reduce_mask: xr.DataArray,
    reduce_groupbys: Tuple,
    expected_groups: Tuple,
    funcname: str,
) -> xr.DataArray:
    print("Starting reduce")
    result = xarray_reduce(
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
    return result


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


def rollup_by_gadm_and_convert_to_aoi(df, groupby_list):
    """Add in extra rows which are aggregates (roll-ups) of the rows over regions and
    countries. groupby_list specifies the contextual layers that are still
    grouped-by. All other columns are aggregated by sum.

    Also convert country/region/subregion columns to an aoi_id field."""

    if df.size == 0:
        df = df.drop(columns=["country", "region", "subregion"])
        df["aoi_id"] = pd.Series(dtype="object")
        df["aoi_type"] = pd.Series(dtype="object")
        return df

    # subregion_df is all the rows which have a subregion. rows that don't have
    # subregion will be covered by region_df and country_df.
    subregion_df = df[df.subregion != 0]

    # Create a dataframe which aggregates by the same groupbys after the subregion
    # column is eliminated - so it is the same grouped results but at the
    # country/region level only. It drops out all rows that don't have a region,
    # since that will be covered by country_df.
    region_df = df.drop(columns=["subregion"])
    region_df = region_df[region_df.region != 0]
    region_df = (
        region_df.groupby(["country", "region"] + groupby_list).sum().reset_index()
    )

    # Create a dataframe which aggregates by the same groupbys after subregion and
    # region area are eliminated - so it is the same grouped results but at the country
    # level only.
    country_df = df.drop(columns=["subregion", "region"])
    country_df = country_df.groupby(["country"] + groupby_list).sum().reset_index()

    # Create aoi_id for rows with country/region/subregion
    if subregion_df.size > 0:
        subregion_df["aoi_id"] = (
            subregion_df[["country", "region", "subregion"]]
            .astype(str)
            .agg(".".join, axis=1)
        )

    # Create aoi_id for rows with only country/region
    if region_df.size > 0:
        region_df["aoi_id"] = (
            region_df[["country", "region"]].astype(str).agg(".".join, axis=1)
        )

    # Create aoi_id for rows with only country.
    country_df["aoi_id"] = country_df["country"]

    subregion_df = subregion_df.drop(columns=["country", "region", "subregion"])
    region_df = region_df.drop(columns=["country", "region"])
    country_df = country_df.drop(columns=["country"])

    # pd.concat will deal fine if region_df or subregion_df are empty, even though
    # they are missing the aoi_id column.
    results_with_ids = pd.concat([country_df, region_df, subregion_df])
    results_with_ids["aoi_type"] = "admin"
    print(
        f"Lengths {len(df)}/{df.size}, {len(subregion_df)}/{subregion_df.size}, {len(region_df)}/{region_df.size}, {len(country_df)}/{country_df.size} -> {len(results_with_ids)}/{results_with_ids.size}"
    )

    return results_with_ids


def save_results(df: pd.DataFrame, results_uri: str) -> str:
    print("Starting parquet")

    _save_parquet(df, results_uri)
    print("Finished parquet")
    return results_uri


# _load_zarr and _save_parquet are the functions being mocked by the unit tests.
def _save_parquet(df: pd.DataFrame, results_uri: str) -> None:
    df.to_parquet(results_uri, index=False)


def _load_zarr(zarr_uri, group=None):
    return xr.open_zarr(zarr_uri, group=group, storage_options={"requester_pays": True})


def _get_tile_uris(tiles_geojson_uri: str) -> List[str]:
    """Extract S3 URIs from a tiles.geojson file."""
    tiles = pd.read_json(tiles_geojson_uri, storage_options={"requester_pays": True})
    return [
        "/".join(["s3:/"] + f["properties"]["name"].split("/")[2:])
        for f in tiles.features
    ]


def create_zarr_from_tiles(
    tiles_geojson_uri: str,
    zarr_uri: str,
    groups: List[Tuple[int, Optional[str]]],
    overwrite: bool = False,
    dtype: Optional[str] = None,
) -> str:
    """Create zarr groups from tiled GeoTIFFs referenced by a tiles.geojson file.

    Tiles are fetched once and written to each requested group, avoiding
    redundant S3 reads when producing multiple chunk-size variants.

    Args:
        tiles_geojson_uri: S3 URI to a tiles.geojson file listing the geotiff tiles.
        zarr_uri: Destination S3 URI for the output zarr store.
        groups: List of (chunk_size, group) pairs describing each zarr group to
            write.  ``chunk_size`` is in pixels; ``group`` is the zarr group
            name (e.g. ``'pipeline'`` or ``'otf'``), or ``None`` for the root.
        overwrite: If True, overwrite existing zarr groups at the target
            location.
        dtype: Optional numpy dtype string (e.g. ``'uint8'``, ``'float64'``) to
            cast all variables to before writing.

    Returns:
        The zarr_uri that was written to.
    """
    groups_to_write = [
        (chunk_size, group)
        for chunk_size, group in groups
        if overwrite
        or not s3_uri_exists(
            f"{zarr_uri}/{group}/zarr.json" if group else f"{zarr_uri}/zarr.json"
        )
    ]
    if not groups_to_write:
        return zarr_uri

    tile_uris = _get_tile_uris(tiles_geojson_uri)
    max_chunk_size = max(chunk_size for chunk_size, _ in groups_to_write)
    dataset = xr.open_mfdataset(
        tile_uris, parallel=True, chunks={"x": max_chunk_size, "y": max_chunk_size}
    ).persist()
    if dtype:
        dataset = dataset.astype(dtype)
    for chunk_size, group in groups_to_write:
        # `mode` applies to group, so `w` won't overwrite the whole store if the group already exists, just that group.
        dataset.chunk({"x": chunk_size, "y": chunk_size}).to_zarr(
            zarr_uri, mode="w", group=group
        )
    return zarr_uri


def create_zarrs(
    datasets: Dict[str, Dict[str, str]],
    overwrite: bool = False,
    pipeline_chunk_size: int = 10_000,
    otf_chunk_size: int = 4_000,
) -> Dict[str, str]:
    """Create zarr stores for multiple datasets from tiled GeoTIFFs.

    Each dataset config must include:
      - ``tiles_uri``: source tiles.geojson URI
      - ``zarr_uri``: destination zarr URI
      - ``dtype``: dtype used before writing
    """
    result_uris: Dict[str, str] = {}

    for name, cfg in datasets.items():
        result_uris[name] = create_zarr_from_tiles(
            cfg["tiles_uri"],
            cfg["zarr_uri"],
            [
                (pipeline_chunk_size, "pipeline"),
                (otf_chunk_size, "otf"),
            ],
            overwrite=overwrite,
            dtype=cfg["dtype"],
        )

    return result_uris
