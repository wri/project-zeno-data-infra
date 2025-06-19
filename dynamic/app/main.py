import json
import uuid
from enum import Enum
from pathlib import Path
from pydoc import describe
from typing import Any, Dict
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi import HTTPException, Request
from pydantic import BaseModel, Extra
from pydantic import Field, root_validator, validator
from typing import Any, Dict, List, Literal, Optional, Union
from uuid import UUID

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

JULIAN_DATE_2015 = 2457033
JULIAN_DATE_DIST_OFFSET = 731

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
    alerts_df.alert_date = pd.to_datetime(alerts_df.alert_date + JULIAN_DATE_2015 + JULIAN_DATE_DIST_OFFSET, origin='julian', unit='D').dt.strftime('%Y-%m-%d')
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
API_URL="http://"
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


class DistAlertsAnalyticsIn(StrictBaseModel):
    aoi: Union[AdminAreaOfInterest] = Field(..., discriminator="type")
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
    intersection: Union[Literal["driver"] | Literal["natural_lands"]]

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

    link = DataMartResourceLink(link=f"{API_URL}v0/land_change/dist_alerts/analytics/{resource_id}")
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

    # Return using your custom response model
    return DistAlertsAnalyticsResponse(data={
        "metadata": metadata_content,
        "status": AnalysisStatus.saved,
    })
