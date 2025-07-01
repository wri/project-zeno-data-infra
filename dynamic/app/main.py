import json
import uuid
from enum import Enum
from pathlib import Path
from pydoc import describe
from typing import Any, Dict, Annotated, List
import duckdb
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi import HTTPException, Request
from pydantic import BaseModel, Extra
from pydantic import Field, root_validator, validator
from typing import Any, Dict, List, Literal, Optional, Union
from uuid import UUID
import requests

from dask.distributed import Client, LocalCluster
from flox.xarray import xarray_reduce
import xarray as xr
import numpy as np
from shapely.geometry import shape
from rasterio import Affine
from rasterio.features import rasterize
from pydantic import BaseModel
from contextlib import asynccontextmanager
import pandas as pd
import os

API_KEY = os.environ["API_KEY"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the dask cluster
    app.state.dask_cluster = LocalCluster(processes=False, asynchronous=True)
    yield
    # Release the resources
    close_call = app.state.dask_cluster.close()
    if close_call is not None:
        await close_call

app = FastAPI(lifespan=lifespan)

class AnalysisInput(BaseModel):
    geojson: Dict[str, Any]
    dataset: str

JULIAN_DATE_2021 = 2459215
NATURAL_LANDS_CLASSES = {
    2: "Forest",
    3: "Short vegetation",
    4: "Water",
    5: "Mangroves",
    6: "Bare",
    7: "Snow/Ice",
    8: "Wetland forest",
    9: "Peat forest",
    10: "Wetland short vegetation",
    11: "Peat short vegetation",
    12: "Cropland",
    13: "Built-up",
    14: "Tree cover",
    15: "Short vegetation",
    16: "Water",
    17: "Wetland tree cover",
    18: "Peat tree cover",
    19: "Wetland short vegetation",
    20: "Peat short vegetation",
    21: "Bare"
}
DIST_DRIVERS = {
    1: "Wildfire",
    2: "Flooding",
    3: "Crop management",
    4: "Potential conversion",
    5: "Unclassified",
}
@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}

# You can add your custom route here
@app.post("/analysis")
def analyze(data: AnalysisInput):
    # rasterize
    geom = shape(data.geojson)

    dist_obj_name = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250510/raster/epsg-4326/zarr/date_conf.zarr"
    dist_alerts = xr.open_zarr(dist_obj_name)

    sliced = dist_alerts.sel(x=slice(geom.bounds[0],geom.bounds[2]), y=slice(geom.bounds[3],geom.bounds[1]),).squeeze("band")
    clipped = sliced.rio.clip([data.geojson])

    alerts_count = xarray_reduce(
        clipped.alert_date, 
        *(
            clipped.alert_date,
            clipped.confidence
        ),
        func='count',
        expected_groups=(
            np.arange(731, 1590),
            [1, 2, 3]
        )
    ).compute()
    alerts_count.name = 'alert_count'

    alerts_df = alerts_count.to_dataframe().drop("band", axis=1).drop("spatial_ref", axis=1).reset_index()
    alerts_df.confidence = alerts_df.confidence.map({2: 'low', 3: 'high'})
    alerts_df.alert_date = pd.to_datetime(alerts_df.alert_date + JULIAN_DATE_2021, origin='julian', unit='D').dt.strftime('%Y-%m-%d')
    alerts_json = alerts_df[alerts_df.alert_count > 0].to_dict(orient="records")

    return {
        "data": alerts_json,
        "status": "success"
    }


######################################################################
# Analytics Endpoint                                                 #
######################################################################

PAYLOAD_STORE_DIR = Path("/tmp/dist_alerts_analytics_payloads")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)
DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"
ADMIN_REGEX = r"^[A-Z]{3}(\.\d+)*$"

class StrictBaseModel(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class DataMartResourceLink(StrictBaseModel):
    link: str


class DataMartResourceLinkResponse(Response):
    data: DataMartResourceLink


class AreaOfInterest(StrictBaseModel):
    async def get_geostore_id(self) -> UUID:
        """Return the unique identifier for the area of interest."""
        raise NotImplementedError("This method is not implemented.")


class AdminAreaOfInterest(AreaOfInterest):
    type: Literal["admin"] = "admin"
    id: str = Field(
        ...,
        title="Dot-delimited identifier",
        pattern=ADMIN_REGEX,
        examples=["BRA.12.3", "IND", "IDN.12"]
    )
    provider: str = Field("gadm", title="Administrative Boundary Provider")
    version: str = Field("4.1", title="Administrative Boundary Version")

    async def get_geostore_id(self) -> UUID:
        admin_level = self.get_admin_level()
        geostore_id = None
        return geostore_id

    def get_admin_level(self):
        admin_level = (
                sum(
                    1
                    for field in (self.country, self.region, self.subregion)
                    if field is not None
                )
                - 1
        )
        return admin_level

    @root_validator(skip_on_failure=True)
    def check_region_subregion(cls, values):
        id = values.get("id")
        # parse id to get region and subregion (if they exist)
        subregion = None
        region = None
        if subregion is not None and region is None:
            raise ValueError("region must be specified if subregion is provided")
        return values

    @validator("provider", pre=True, always=True)
    def set_provider_default(cls, v):
        return v or "gadm"

    @validator("version", pre=True, always=True)
    def set_version_default(cls, v):
        return v or "4.1"


class KeyBiodiversityAreaOfInterest(AreaOfInterest):
    type: Literal["key_biodiversity_area"] = "key_biodiversity_area"
    id: int = Field(
        ...,
        title="Key Biodiversity Area site code",
        examples=[36, 18, 8111]
    )


class ProtectedAreaOfInterest(AreaOfInterest):
    type: Literal["protected_area"] = "protected_area"
    id: int = Field(
        ...,
        title="WDPA protected area ID",
        examples=[555625448, 148322, 555737674]
    )


class IndigneousAreaOfInterest(AreaOfInterest):
    type: Literal["indigenous_land"] = "indigenous_land"
    id: int = Field(
        ...,
        title="Landmark Indigenous lands object ID",
        examples=[1931, 1918, 43053]
    )

class CustomAreaOfInterest(AreaOfInterest):
    type: Literal["geojson"] = "geojson"
    geojson: Dict[str, Any] = Field(
        ...,
        title="GeoJSON of one geometry",
    )

AoiUnion = Union[AdminAreaOfInterest, KeyBiodiversityAreaOfInterest, ProtectedAreaOfInterest, IndigneousAreaOfInterest, CustomAreaOfInterest]

class DistAlertsAnalyticsIn(StrictBaseModel):
    aois: List[Annotated[AoiUnion, Field(discriminator="type")]] = Field(
        ...,
        min_items=1,
        max_items=1,
        description="List of areas of interest."
    )
    start_date: str = Field(
        ...,
        title="Start Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2020", "2020-01-01"]
    )
    end_date: str = Field(
        ...,
        title="End Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2023", "2023-12-31"]
    )
    intersections: List[Literal["driver", "natural_lands"]] = Field(
        ...,
        min_items=0,
        max_items=1,
        description="List of intersection types"
    )

@app.post("/v0/land_change/dist_alerts/analytics",
          response_class=ORJSONResponse,
          response_model=DataMartResourceLinkResponse,
          tags=["Beta LandChange"],
          status_code=202)
def create(
    *,
    data: DistAlertsAnalyticsIn,
    request: Request,
):
    """
    # Primary Dataset
    | Dataset Key Aspects | Detailed Description |
    |---------------------|----------------------|
    | **Title** | Global all ecosystem disturbance alerts (DIST-ALERT) |
    | **Description** <br> A descriptive summary of what the dataset is and key details, such as information on geographic coverage, temporal range, and the data sources used. | Monitors global vegetation disturbance in near-real-time using harmonized Landsat-Sentinel-2 (HLS) imagery |
    | **Function / usage notes in Zeno** <br> The intended use of the dataset. How might users employ this data in order to have some impact in the world?<br>When should Zeno suggest/use this dataset? Please include examples of use cases (e.g. X dataset can be used for XYZ). You can also include these examples in the prompt spreadsheet. | Users are informed about significant changes to the vegetation, which can indicate human induced changes such as deforestation, as well as drought and other events where actions by land managers may be required. Crucially, this dataset covers all vegetation, not just forest, so that new non-conversion of natural ecosystems commitments can be monitored. |
    | **Methodology** <br> How was this dataset created? | This dataset is a derivative of the OPERA’s DIST-ALERT product (OPERA Land Surface Disturbance Alert from Harmonized Landsat Sentinel-2 product), which is derived through a time-series analysis of harmonized data from the NASA/USGS Landsat and ESA Sentinel-2 satellites (known as the HLS dataset). The product identifies and continuously monitors vegetation cover change in 30-m pixels across the globe. It can be accessed through the LPDAAC website here: Earthdata Search , and on Google Earth Engine (GEE) as an Image Collection with asset ID: projects/glad/HLSDIST/current/[Insert Layer Name].<br>The DIST-ALERT on GFW is a derivative of this, and additional data layers not used in the GFW product are available through the LPDAAC and GEE such as initial vegetation fraction, and disturbance duration. While the version on the LPDAAC is updated every 2-4 days, the data is updated weekly on GFW.  The product detects notable reductions in vegetation cover (measured as “vegetation fraction” or the percent of the ground that is covered by vegetation) for every pixel every time the new satellite data is acquired and the ground is not obscured by clouds or snow.  <br>The current vegetation fraction estimate is compared to the minimum fraction for the same time period (within 15 days before and after) in the previous 3 years, and if there is a reduction, then the system identifies an alert in that pixel. Anomalies of at least a 10% reduction from the minimum become alerts in the original product, and on GFW, a higher threshold of 30% is used, to reduce noise, and false alerts in the dataset. Because the product compares each pixel to the minimum for the same time period in previous years, it takes into account regular seasonal variation in vegetation cover. As the product is global and detects vegetation anomalies, much of the data may not be applicable to GFW users monitoring forests. Therefore, we also offer the option to mask the alerts with UMD’s tree cover map, allowing users to view only alerts within 30% canopy cover. |
    | **Cautions in Zeno**<br>What should be kept in mind when using this dataset?<br>When should Zeno not suggest/use this dataset? Feel free to include examples of when the data should not be used (e.g. X dataset cannot be used for XYZ).<br>Generates a persistent resource link for deforestation alerts analytics requests. | These alerts detect vegetation cover loss or disturbance. This product does not distinguish between human-caused and other disturbance types. For example, where alerts are detected within plantation forests, alerts may indicate timber harvesting operations, without a conversion to a non-forest land use, and when alerts are detected within crop land, alerts may represent crop harvesting<br>We do not recommend using the alerts for global or regional trend assessment, nor for area estimates. Rather, we recommend using the annual tree cover loss data for a more accurate comparison of the trends in forest change over time, and for area estimates. For global land cover changes, 20 year data may be more useful for certain purposes and should be explored. Additionally, updates to the methodologies and variation in cloud cover between months and years pose additional risks to using alerts for inter/intra-annual comparison.<br>The alerts can be ‘curated’ to identify those alerts of interest to a user, such as those alerts which are likely to be deforestation and might be prioritized for action. A user can do this by overlaying other contextual datasets, such as protected areas, or planted trees. The non-curated data are provided here in order that users can define their own prioritization approaches. Curated alert locations within tree cover are provided in the Places to Watch data layer.<br>We provide a masked version of the product within “tree cover” which is defined as all vegetation greater than 5 meters in height (2020) with greater than 30% canopy cover (2010), and may take the form of natural forests or plantations. Annual tree cover loss from 2021 is masked out. <br>In contrast to other alert systems available on GFW, DIST-ALERT continues to monitor pixels where it has identified an alert in the past. The DIST-ALERT retains the date of the most recent detection of disturbance, keeping users informed of the most up-to-date changes within tree cover.<br>Two confidence levels are provided. The approach determines confidence level by the number of anomalous observations, with more observations meaning a higher confidence level. That is, two to three anomalies detected result in a low confidence alert, whereas four or more mean a high confidence alert. UMD’s Google Earth Engine app DIST-ALERT ) displays alerts, with a different approach used to define confidence, and a different threshold for the vegetation reduction which triggers alerts. |
    | **Resolution** | 30 x 30 meters |
    | **Geographic Coverage** | Global |
    | **Update Frequency** | Underlying product updated daily, with image revisit time every 2-4 days. Product on GFW updated weekly. |
    | **Content Date** | 1 January 2023 – present (GFW has data since 1 December 2023, and in future, will display only the most recent 2 years of alert data) |
    | **Keywords** | land cover, alerts, deforestation, conversion, vegetation disturbance |
    | **Band values/meaning**<br>What does the pixel values of each band mean? What are the possible values? | The input layers and their encoding can be found in the documentation<br>Single bands<br>Type: discrete<br>default.tif (formula is (10000 * confidence) + (days since Dec 31, 2020))<br>20000-29999: Low confidence (Ex. 21431 = Low confidence alert on Dec 1, 2024)<br>30000-39999: High confidence (Ex. 31433 = High confidence alert on Dec 3, 2024)<br>intensity.tif<br>All alerts set to 255 and use bilinear resampling on the overviews so that areas with dense alerts have higher values<br>Possible values of the final GFW product:<br>either low or high confidence, based on the alert count<br>date of the alert based on the VEG-DIST-DATE (this represents the Day of first loss anomaly detection of the most recent disturbance event. Day denoted as the number of days since December 31, 2020.) |

    -----
    # Available Intersection Datasets
    ## Drivers (LDACS)
    | Dataset Key Aspects | Detailed Description |
    |---------------------|----------------------|
    | **Title** | Land Disturbance Alert Classification System (LDACS) |
    | **Description** <br>A descriptive summary of what the dataset is and key details, such as information on geographic coverage, temporal range, and the data sources used. | The LDACS classifies the drivers of global vegetation disturbance alerts (DIST-ALERT)(Pickens, Hansen, and Song, 2024) on a quarterly basis using a rule-based approach. Alerts are categorized as potential conversion, wildfires, surface water dynamics, or crop management. This system integrates DIST-ALERT data with thematic layers on fire activity (NASA VIIRS Fire Alerts), surface water dynamics (Pickens et al. 2020), cropland extent (UMD Global Cropland Dynamics (Potapov et al. 2022); ESA WorldCover (Zanaga et al. 2020)), and natural land cover (SBTN Natural Lands), using a rule-based approach to assign probable causes to alerts. Each run of the system processes DIST-ALERT data from the previous 12 months and is updated quarterly. |
    | **Function / usage notes in Zeno**<br>The intended use of the dataset. How might users employ this data in order to have some impact in the world?<br>When should Zeno suggest/use this dataset? Please include examples of use cases (e.g. X dataset can be used for XYZ). You can also include these examples in the prompt spreadsheet. | The LDACS is designed to help users distinguish between disturbances caused by human activity and those resulting from natural events. DIST-ALERTs are assigned to one of five categories: potential conversion, crop management, wildfire, surface water dynamics, or unclassified. The LDACS simplifies the process of identifying alerts that are likely caused by land conversion, with the goal of supporting broader adoption of DIST-ALERT among governments, companies, and conservation organizations. <br>Zeno should use this dataset when a user is interested in why a vegetation disturbance alert has occurred. The LDACS dataset has global coverage and therefore, can be used across all vegetated ecosystems. LDACS data will be made available for the most recent 12-month period and will be updated quarterly. |
    | **Methodology**<br>How was this dataset created? | The Land Disturbance Alert Classification System was developed by combining DIST-ALERT data with a set of thematic data masks derived from multiple geospatial data sources. These masks were used to infer the most likely driver of each vegetation disturbance alert. <br>Each run of the system processes DIST-ALERT data from the previous 12 months, using the alert start date (VEG-DIST-DATE) to filter the data, to ensure that classifications are based on recent and relevant disturbance activity. Because each run processes a rolling 12-month window, a single disturbance event may appear in multiple consecutive quarterly runs (up to three or four times), depending on the timing of its detection. Only DIST-ALERTS that fall under the following criteria were classified under this system: VEG-DIST-COUNT >= 2 and VEG-ANOM-MAX > 50, meaning alerts with less than 50% vegetation loss or fewer than two cloud-free satellite observations are excluded to minimize false positives. <br>The following paragraphs describe the classification logic for each disturbance category:<br>Wildfire: DIST-ALERTs were flagged as fire-related if they occurred within the three months after, and within 3 kilometers of, a VIIRS fire alert, and were located within natural lands as defined by the SBTN Natural Lands Map.<br>Flooding: To identify alert pixels likely associated with surface water dynamics, we used annual surface water dynamic composites (Pickens et al. 2020) to create a binary mask highlighting all pixels where surface water was detected at least once between 1999 and 2021. This mask identifies areas with a history of surface water presence and helps flag locations prone to periodic or irregular inundation. Alerts that fall within the surface water dynamics mask are flagged and classified as flooding. <br>Crop management: DIST-ALERT pixels located within areas mapped as cropland by either the UMD Global Cropland Dynamics dataset (Potapov et al. 2022) or the ESA WorldCover dataset (Zanaga et al. 2020) were classified as crop management changes.<br>Potential conversion: Potential conversion events were then assessed using two metrics from the DIST-ALERT data set: maximum vegetation anomaly (VEG-ANOM-MAX) and alert duration (VEG-DIST-DUR). Alerts were flagged as potential conversion if they occurred in natural lands (SBTN Natural Lands), showed a vegetation loss greater than 50% at any point during the alert period (VEG-ANOM-MAX > 50), and that loss persisted for at least 75 days (VEG-DIST-DUR >= 75). The potential conversion alert pixels were post-processed using morphological operations, a common technique in satellite image analysis to clean up pixel-based maps (Haralick, Sternberg, and Zhuang 1987; Löw et al. 2022; d’Andrimont et al. 2020). Specifically, erosion was applied to remove small or narrow clusters of pixels that may represent noise or false detections. Dilation was then applied to expand the remaining patches outward by one pixel in all directions. While this step helps restore the general shape and spatial extent of larger disturbance areas, it produces a smoothed approximation of the original patch, helping eliminate small gaps and jagged edges while maintaining the overall structure.<br>The resulting classes are merged into a final classification layer using a hierarchical scheme: wildfire takes the highest priority, followed by flooding, crop management, and potential conversion.<br>Unclassified: Alerts that do not meet the criteria for any of the four categories above are assigned to a fifth class, labeled “unclassified” alerts. |
    | **Cautions in Zeno**<br>What should be kept in mind when using this dataset?<br>When should Zeno not suggest/use this dataset? Feel free to include examples of when the data should not be used (e.g. X dataset cannot be used for XYZ). | In contrast to other alert systems, DIST-ALERT continues to monitor pixels where it has identified an alert in the past. The DIST-ALERT retains the date of the most recent detection of disturbance, keeping users informed of the most up-to-date changes.<br>Several input datasets used to produce the LDACS contain temporal gaps or known mapping errors that may affect classification accuracy. Please refer to the technical note for more information on the limitations of the input datasets.<br>The classification system uses a static version of the SBTN natural lands map to identify potential conversion events. This layer is not updated in response to disturbances flagged by DIST-ALERT. For example, if a wildfire occurs in a natural area and is followed by land clearing, the system will still treat the area as natural land if it remains within the boundaries of the original natural lands data set. Updates to this map only occur when the source data set itself is revised, which may result in alerts being evaluated against outdated land use information. As a result, disturbances that alter the ecological state of the land (e.g., severe fire or tree cover loss) are not reflected in the natural lands mask used for classification, potentially contributing to inclusion or exclusion errors depending on the disturbance sequence and timing.<br>Because potential conversion is identified based on the duration of vegetation loss, there is an inherent delay in labeling these alerts. Although vegetation loss may be detected shortly after clearing, alerts are not assigned to the potential conversion class until they exceed the 75-day duration threshold. This delay allows for greater confidence in classification but may reduce the timeliness of conversion detection for rapid response. <br>LDACS data will be made available for the most recent 12-month period and will be updated quarterly. Data prior to this 12-month period will not be available. Additionally, there may be up to a 3-month delay in LDACS data until the quarterly update occurs.<br>LDACS classifies DIST-ALERTs with a minimum of VEG-DIST-COUNT >= 2 and VEG-ANOM-MAX > 50. The DIST-ALERT layer on Zeno has a minimum of VEG-DIST-COUNT >-2 and VEG-ANOM-MAX >= 30, therefore, LDACS classifies a subset of these alerts. DIST-ALERTs with a VEG-ANOM-MAX between 30 to 50 will not be classified through the LDACS. |
    | **Resolution** | 30 x 30 meters |
    | **Geographic Coverage** | Global |
    | **Update Frequency** | Quarterly |
    | **Content Date** | Alerts evaluated over previous 12-month period, updated quarterly. |
    | **Keywords** | alerts, deforestation, conversion, vegetation disturbance, drivers of disturbance |
    | **Band values/meaning**<br>What does the pixel values of each band mean? What are the possible values? | Single band<br>Type: classification<br>Possible values:<br>1 = Wildfire<br>2 = Flooding<br>3 = Crop management<br>4 = Potential conversion<br>5 = Unclassified |

    -----

    ## Natural Lands (SBTN)
    | Dataset Key Aspects | Detailed Description |
    |---------------------|----------------------|
    | **Title** | SBTN Natural Lands Map |
    | **Description**<br>A descriptive summary of what the dataset is and key details, such as information on geographic coverage, temporal range, and the data sources used. | The SBTN Natural Lands Map v1.1 is a 2020 baseline map of natural and non-natural land covers intended for use by companies setting science-based targets for nature, specifically the SBTN Land target #1: no conversion of natural ecosystems.<br>This map is global with 30m resolution and was made by compiling existing global and regional data including the GLAD Global Land Cover and Change data, ESA WorldCover, and many other land cover and use datasets.  <br"Natural" and "non-natural" definitions were adapted from the Accountability Framework initiative's definition of a natural ecosystem as "one that substantially resembles - in terms of species composition, structure, and ecological function - what would be found in a given area in the absence of major human impacts" and can include managed ecosystems as well as degraded ecosystems that are expected to regenerate either naturally or through management (AFi 2024). The SBTN Natural Lands Map operationalizes this definition by using proxies based on available data that align with AFi guidance to the extent possible. |
    | **Function / usage notes in Zeno**<br>The intended use of the dataset. How might users employ this data in order to have some impact in the world?<br>When should Zeno suggest/use this dataset? Please include examples of use cases (e.g. X dataset can be used for XYZ). You can also include these examples in the prompt spreadsheet. | The Natural Lands Map was created for companies to use as a baseline when identifying if there has been conversion of natural ecosystems in their supply chains since 2020. |
    | **Methodology<br>How was this dataset created? | The SBTN Natural Lands Map is a compilation of many global and regional land cover and land use datasets through a decision tree to disaggregate natural land from non-natural lands, according to definition of natural by the Accountability Framework. Global land cover and use datasets were overlaid with a hierarchy by land cover class, and then regional data replaced the global data, where available. The binary natural/non-natural classes were validated independently by IIASA. |
    | **Cautions in Zeno**<br>What should be kept in mind when using this dataset?<br>When should Zeno not suggest/use this dataset? Feel free to include examples of when the data should not be used (e.g. X dataset cannot be used for XYZ). | "Natural" and "non-natural" definitions were adapted from the Accountability Framework initiative's definition of a natural ecosystem as "one that substantially resembles - in terms of species composition, structure, and ecological function - what would be found in a given area in the absence of major human impacts" and can include managed ecosystems as well as degraded ecosystems that are expected to regenerate either naturally or through management (AFi 2024). The SBTN Natural Lands Map operationalizes this definition by using proxies based on available data that align with AFi guidance to the extent possible. These definitions may not match all uses of the term “natural” so users should exercise caution when using this map out of the conversion-free supply chains context. <br>This map overestimates the extent of natural lands, and while remote sensing data, on which the map is based, can provide powerful insights, additional field work should be used for validation and to understand local dynamics. Caution should be used if calculating areas with the SBTN Natural Lands Map. |
    | **Resolution** | 30 x 30 meters |
    | **Geographic Coverage** | Global |
    | **Update Frequency** | Irregular |
    | **Content Date** | 2020 |
    | **Keywords** | land cover, deforestation, conversion, supply chains, natural lands |
    | **Band values/meaning**<br>What does the pixel values of each band mean? What are the possible values? | Single band<br>Type: classification<br>binary<br>0: Non-natural<br>1: Natural<br>classification<br>[2-11] natural land classes<br>2: natural forests<br>3: natural short vegetation<br>4: natural water<br>5: mangroves<br>6: bare<br>7: snow<br>8: wetland natural forests<br>9: natural peat forests<br>10: wetland natural short vegetation<br>11: natural peat short vegetation<br>[12-21] non-natural classes<br>12: cropland<br>13: built-up<br>14: non-natural tree cover<br>15: non-natural short vegetation<br>16: non-natural water<br>17: wetland non-natural tree cover <br>18: non-natural peat tree cover<br>19: wetland non-natural short vegetation<br>20: non-natural peat short vegetation<br>21: non-natural bare |

    -----

    **Key Features:**
    - 🆔 Deterministic UUID generation using SHA-1 hashing (UUIDv5)
    - 💾 Automatic payload storage in temporary storage
    - 🔗 Returns a URL to check analytics status
    - ♻️ Idempotent: Identical payloads return the same resource ID

    **Flow:**
    1. Accepts `DistAlertsAnalyticsIn` payload
    2. Generates UUID based on payload content
    3. Stores payload as JSON file
    4. Returns resource URL for status checking
    """

    # Convert model to JSON with sorted keys
    payload_dict = data.model_dump()

    # Convert to JSON string with sorted keys
    payload_json = json.dumps(payload_dict, sort_keys=True)

    # Generate deterministic UUID from payload
    resource_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, payload_json))

    # Store payload in /tmp directory
    payload_file = PAYLOAD_STORE_DIR / f"{resource_id}.json"
    payload_file.write_text(payload_json)

    link = DataMartResourceLink(link=f"{str(request.base_url).rstrip('/')}/v0/land_change/dist_alerts/analytics/{resource_id}")
    return DataMartResourceLinkResponse(data=link)


class AnalysisStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"


class DistAlertsAnalytics(StrictBaseModel):
    result: Optional[dict] = {  # column oriented for loading into a dataframe
        "__dtypes__": {
            "country": "str",
            "region": "int",
            "subregion": "int",
            "alert_date": "int",
            "confidence": "int",
            "value": "int",
        },
        "country": ["BRA", "BRA", "BRA"],
        "region": [1, 1, 1],
        "subregion": [12, 12, 12],
        "alert_date": [731, 733, 733],
        "confidence": [2, 2, 3],
        "value": [38, 5, 3],
    }
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: AnalysisStatus

    class Config:
        orm_mode = True
        allow_population_by_field_name = True

class DistAlertsAnalyticsResponse(Response):
    data: DistAlertsAnalytics


@app.get("/v0/land_change/dist_alerts/analytics/{resource_id}",
         response_class=ORJSONResponse,
         response_model=DistAlertsAnalyticsResponse,
         tags=["Beta LandChange"],
         status_code=200)
async def get_analytics_result(resource_id: str):
    # Validate UUID format
    try:
        uuid.UUID(resource_id)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid resource ID format. Must be a valid UUID."
        )

    # Construct file path
    file_path = PAYLOAD_STORE_DIR / f"{resource_id}.json"

    # Check if file exists
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Requested resource not found. Either expired or never existed."
        )

    # Read and parse JSON file
    json_content = file_path.read_text()
    metadata_content = json.loads(json_content)  # Convert JSON to Python object

    aoi = metadata_content["aois"][0]
    if aoi["type"] == "admin":
        # GADM IDs are coming joined by '.', e.g. IDN.24.9
        gadm_ids = aoi["id"].split(".")
        intersections = metadata_content["intersections"]

        # Each intersection will be in a different parquet file
        if not intersections:
            table = "gadm_dist_alerts"
        elif intersections[0] == "driver":
            table = "gadm_dist_alerts_by_driver"
            intersection_col = "ldacs_driver"
        elif intersections[0] == "natural_lands":
            table = "gadm_dist_alerts_by_natural_lands"
            intersection_col = "natural_land_class"
        else:
            raise ValueError(f"No way to calculate intersection {intersections[0]}")
        
        # TODO use some better pattern here is so it doesn't become spaghetti once we have more datasets. ORM?
        # TODO use final pipeline locations and schema for parquet files 
        # TODO this should be done in a background task and written to file
        # Build up the DuckDB query based on GADM ID and intersection
        from_clause = f"FROM 's3://gfw-data-lake/umd_glad_dist_alerts/parquet/{table}.parquet'"
        select_clause = "SELECT country"
        where_clause = f"WHERE country = '{gadm_ids[0]}'"
        group_by_clause = f"GROUP BY country"

        # Includes region, so add relevant filters, selects and group bys
        if len(gadm_ids) > 1:
            select_clause += ", region"
            where_clause += f" AND region = '{gadm_ids[1]}'"
            group_by_clause += ", region"

        # Includes subregion, so add relevant filters, selects and group bys
        if len(gadm_ids) > 2:
            select_clause += ", subregion"
            where_clause += f" AND subregion = '{gadm_ids[2]}'"
            group_by_clause += ", subregion"

        # Includes an intersection, so group by the appropriate column
        if intersections:
            select_clause += f", {intersection_col}"
            group_by_clause += f", {intersection_col}"

        group_by_clause += ", alert_date, alert_confidence"
        
        # Query and make sure output names match the expected schema (?)
        select_clause += ", alert_date, alert_confidence as confidence, sum(count) as value"
        query = f"{select_clause} {from_clause} {where_clause} {group_by_clause}"
        
        # Dumbly doing this per request since the STS token expires eventually otherwise
        # According to this issue, duckdb should auto refresh the token in 1.3.0, 
        # but it doesn't seem to work for us and people are reporting the same on the issue
        # https://github.com/duckdb/duckdb-aws/issues/26
        # TODO do this on lifecycle start once autorefresh works
        duckdb.query('''
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain
            );
        ''')

        # Send query to DuckDB and convert to return format
        alerts_df = duckdb.query(query).df()
    else:
        if aoi["type"] == "geojson":
            geojson = aoi["geojson"]
        else:
            if aoi["type"] == "key_biodiversity_area":
                url = f"https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query?sql=select gfw_geojson from data where sitrecid = {aoi['id']}&x-api-key={API_KEY}"
            elif aoi["type"] == "protected_area":
                url = f"https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query?sql=select gfw_geojson from data where wdpaid = {aoi['id']}&x-api-key={API_KEY}"
            elif aoi["type"] == "indigenous_land":
                url = f"https://data-api.globalforestwatch.org/dataset/landmark_icls/latest/query?sql=select gfw_geojson from data where objectid = {aoi['id']}&x-api-key={API_KEY}"

            response = requests.get(url)
            response = response.json()
            if response["data"]:
                geojson = json.loads(response["data"][0]["gfw_geojson"])

        if metadata_content["intersections"]:
            intersection = metadata_content["intersections"][0]
        else:
            intersection = None

        alerts_df = await _analyze(geojson, aoi, intersection)
        
    if metadata_content["start_date"] is not None:
        alerts_df = alerts_df[alerts_df.alert_date >= metadata_content["start_date"]]
    if metadata_content["end_date"] is not None:
        alerts_df = alerts_df[alerts_df.alert_date <= metadata_content["end_date"]]

    alerts_dict = alerts_df.to_dict(orient="list")

    # Return using your custom response model
    return DistAlertsAnalyticsResponse(data={
        "result": alerts_dict,
        "metadata": metadata_content,
        "status": AnalysisStatus.saved,
    })


async def _analyze(geojson, aoi, intersection=None):
    dist_obj_name = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250510/raster/epsg-4326/zarr/date_conf.zarr"
    dist_alerts = _clip_xarr_to_geojson(xr.open_zarr(dist_obj_name), geojson)

    groupby_layers = [dist_alerts.alert_date, dist_alerts.confidence]
    expected_groups = [np.arange(731, 1590), [1, 2, 3]]
    if intersection == 'natural_lands':
        natural_lands = _clip_xarr_to_geojson(xr.open_zarr(
            's3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes_clipped_to_dist.zarr'
        ).band_data, geojson)
        natural_lands.name = "natural_land_class"

        groupby_layers.append(natural_lands)
        expected_groups.append(np.arange(22))
    elif intersection == 'driver':
        dist_drivers = _clip_xarr_to_geojson(xr.open_zarr(
            "s3://gfw-data-lake/umd_glad_dist_alerts_driver/zarr/umd_dist_alerts_drivers.zarr"
        ).band_data, geojson)
        dist_drivers.name = "ldacs_driver"

        groupby_layers.append(dist_drivers) 
        expected_groups.append(np.arange(5))

    alerts_count = xarray_reduce(
        dist_alerts.alert_date, 
        *tuple(groupby_layers),
        func='count',
        expected_groups=tuple(expected_groups),
    ).compute()
    alerts_count.name = 'value'

    alerts_df = alerts_count.to_dataframe().drop("band", axis=1).drop("spatial_ref", axis=1).reset_index()
    alerts_df.confidence = alerts_df.confidence.map({2: 'low', 3: 'high'})
    alerts_df.alert_date = pd.to_datetime(alerts_df.alert_date + JULIAN_DATE_2021, origin='julian', unit='D').dt.strftime('%Y-%m-%d')
    
    if "id" in aoi:
        alerts_df[aoi["type"]] = aoi["id"]
    
    
    if intersection == 'natural_lands':
        alerts_df.natural_land_class = alerts_df.natural_land_class.apply(lambda x: NATURAL_LANDS_CLASSES.get(x, 'Unclassified'))
    elif intersection == 'driver':
        alerts_df.ldacs_driver = alerts_df.ldacs_driver.apply(lambda x: DIST_DRIVERS.get(x, 'Unclassified'))

    alerts_df = alerts_df[alerts_df.value > 0]
    return alerts_df


def _clip_xarr_to_geojson(xarr, geojson):
    geom = shape(geojson)
    sliced = xarr.sel(x=slice(geom.bounds[0],geom.bounds[2]), y=slice(geom.bounds[3],geom.bounds[1]),).squeeze("band")
    clipped = sliced.rio.clip([geojson])
    return clipped