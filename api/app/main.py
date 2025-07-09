import json
import uuid
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional, Union
from uuid import UUID

import duckdb
import numpy as np
import pandas as pd
import xarray as xr
from dask.distributed import LocalCluster
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi import Response as FastAPIResponse
from fastapi.responses import ORJSONResponse
from flox.xarray import xarray_reduce
from pydantic import BaseModel, Field, model_validator, field_validator
from shapely.geometry import shape

from api.app.analysis import JULIAN_DATE_2021, get_geojson, zonal_statistics
from api.app.query import create_gadm_dist_query
from api.app.routers import dist_alerts


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

app.include_router(dist_alerts.router)


class AnalysisInput(BaseModel):
    geojson: Dict[str, Any]
    dataset: str


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

    sliced = dist_alerts.sel(
        x=slice(geom.bounds[0], geom.bounds[2]),
        y=slice(geom.bounds[3], geom.bounds[1]),
    ).squeeze("band")
    clipped = sliced.rio.clip([data.geojson])

    alerts_count = xarray_reduce(
        clipped.alert_date,
        *(clipped.alert_date, clipped.confidence),
        func="count",
        expected_groups=(np.arange(731, 1590), [1, 2, 3]),
    ).compute()
    alerts_count.name = "alert_count"

    alerts_df = (
        alerts_count.to_dataframe()
        .drop("band", axis=1)
        .drop("spatial_ref", axis=1)
        .reset_index()
    )
    alerts_df.confidence = alerts_df.confidence.map({2: "low", 3: "high"})
    alerts_df.alert_date = pd.to_datetime(
        alerts_df.alert_date + JULIAN_DATE_2021, origin="julian", unit="D"
    ).dt.strftime("%Y-%m-%d")
    alerts_json = alerts_df[alerts_df.alert_count > 0].to_dict(orient="records")

    return {"data": alerts_json, "status": "success"}


######################################################################
# Analytics Endpoint                                                 #
######################################################################

PAYLOAD_STORE_DIR = Path("/tmp/dist_alerts_analytics_payloads")
PAYLOAD_STORE_DIR.mkdir(parents=True, exist_ok=True)
DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"
ADMIN_REGEX = r"^[A-Z]{3}(\.\d+)*$"


class StrictBaseModel(BaseModel):
    model_config = {
        'extra': 'forbid',
        'validate_assignment': True,
    }


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class DataMartResourceLink(StrictBaseModel):
    link: str


class DataMartResourceLinkResponse(Response):
    data: DataMartResourceLink


class AreaOfInterest(StrictBaseModel):
    async def get_geostore_id(self) -> Optional[UUID]:
        """Return the unique identifier for the area of interest."""
        raise NotImplementedError("This method is not implemented.")


class AdminAreaOfInterest(AreaOfInterest):
    type: Literal["admin"] = "admin"
    id: str = Field(
        ...,
        title="Dot-delimited identifier",
        pattern=ADMIN_REGEX,
        examples=["BRA.12.3", "IND", "IDN.12"],
    )
    provider: str = Field("gadm", title="Administrative Boundary Provider")
    version: str = Field("4.1", title="Administrative Boundary Version")

    async def get_geostore_id(self) -> Optional[UUID]:
        # admin_level = self.get_admin_level()
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

    @model_validator(mode='after')
    def check_region_subregion(cls, values):
        # id = values.get("id")
        # parse id to get region and subregion (if they exist)
        subregion = None
        region = None
        if subregion is not None and region is None:
            raise ValueError("region must be specified if subregion is provided")
        return values

    @field_validator("provider", mode='before')
    def set_provider_default(cls, v):
        return v or "gadm"

    @field_validator("version", mode='before')
    def set_version_default(cls, v):
        return v or "4.1"


class KeyBiodiversityAreaOfInterest(AreaOfInterest):
    type: Literal["key_biodiversity_area"] = "key_biodiversity_area"
    id: str = Field(
        ..., title="Key Biodiversity Area site code", examples=["36", "18", "8111"]
    )


class ProtectedAreaOfInterest(AreaOfInterest):
    type: Literal["protected_area"] = "protected_area"
    id: str = Field(
        ...,
        title="WDPA protected area ID",
        examples=["555625448", "148322", "555737674"],
    )


class IndigneousAreaOfInterest(AreaOfInterest):
    type: Literal["indigenous_land"] = "indigenous_land"
    id: str = Field(
        ...,
        title="Landmark Indigenous lands object ID",
        examples=["1931", "1918", "43053"],
    )


class CustomAreaOfInterest(AreaOfInterest):
    type: Literal["geojson"] = "geojson"
    geojson: Dict[str, Any] = Field(
        ...,
        title="GeoJSON of one geometry",
    )


AoiUnion = Union[
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
    ProtectedAreaOfInterest,
    IndigneousAreaOfInterest,
    CustomAreaOfInterest,
]


class DistAlertsAnalyticsIn(StrictBaseModel):
    aois: List[Annotated[AoiUnion, Field(discriminator="type")]] = Field(
        ..., min_length=1, max_length=1, description="List of areas of interest."
    )
    start_date: str = Field(
        ...,
        title="Start Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2020", "2020-01-01"],
    )
    end_date: str = Field(
        ...,
        title="End Date",
        description="Must be either year or YYYY-MM-DD date format.",
        pattern=DATE_REGEX,
        examples=["2023", "2023-12-31"],
    )
    intersections: List[Literal["driver", "natural_lands"]] = Field(
        ..., min_length=0, max_length=1, description="List of intersection types"
    )


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

    model_config = {
        'from_attributes': True,
        'validate_by_name': True,
    }


class DistAlertsAnalyticsResponse(Response):
    data: DistAlertsAnalytics


@app.get(
    "/v0/land_change/dist_alerts/analytics/{resource_id}",
    response_class=ORJSONResponse,
    response_model=DistAlertsAnalyticsResponse,
    tags=["Beta LandChange"],
    status_code=200,
)
async def get_analytics_result(
        resource_id: str,
        response: FastAPIResponse,
):
    # Validate UUID format
    try:
        uuid.UUID(resource_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Invalid resource ID format. Must be a valid UUID."
        )

    # Construct file path
    file_path = PAYLOAD_STORE_DIR / resource_id
    analytics_metadata = file_path / "metadata.json"
    analytics_data = file_path / "data.json"
    metadata_content = None
    alerts_dict = None

    if analytics_metadata.exists() and analytics_data.exists():
        # load resource from filesystem
        alerts_dict = json.loads(analytics_data.read_text())
        metadata_content = json.loads(analytics_metadata.read_text())

        return DistAlertsAnalyticsResponse(
            data={
                "result": alerts_dict,
                "metadata": metadata_content,
                "status": AnalysisStatus.saved,
            },
            status="success",
        )

    if metadata_content:
        response.headers["Retry-After"] = '1'
        return DistAlertsAnalyticsResponse(
            data={
                "status": AnalysisStatus.pending,
                "message": "Resource is still processing, follow Retry-After header.",
                "result": alerts_dict,
                "metadata": metadata_content,
            },
            status="success",
        )


    if not analytics_metadata.exists():
        raise HTTPException(
            status_code=404,
            detail="Requested resource not found. Either expired or never existed.",
        )


async def do_analytics(file_path):
    # Read and parse JSON file
    metadata = file_path / "metadata.json"
    json_content = metadata.read_text()
    metadata_content = json.loads(json_content)  # Convert JSON to Python object
    aoi = metadata_content["aois"][0]
    if aoi["type"] == "admin":
        # GADM IDs are coming joined by '.', e.g. IDN.24.9
        gadm_id = aoi["id"].split(".")
        intersections = metadata_content["intersections"]

        query = create_gadm_dist_query(gadm_id, intersections)

        # Dumbly doing this per request since the STS token expires eventually otherwise
        # According to this issue, duckdb should auto refresh the token in 1.3.0,
        # but it doesn't seem to work for us and people are reporting the same on the issue
        # https://github.com/duckdb/duckdb-aws/issues/26
        # TODO do this on lifecycle start once autorefresh works
        duckdb.query(
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain
            );
        """
        )

        # Send query to DuckDB and convert to return format
        alerts_df = duckdb.query(query).df()
    else:
        geojson = await get_geojson(aoi)

        if metadata_content["intersections"]:
            intersection = metadata_content["intersections"][0]
        else:
            intersection = None

        alerts_df = await zonal_statistics(geojson, aoi, intersection)
    if metadata_content["start_date"] is not None:
        alerts_df = alerts_df[alerts_df.alert_date >= metadata_content["start_date"]]
    if metadata_content["end_date"] is not None:
        alerts_df = alerts_df[alerts_df.alert_date <= metadata_content["end_date"]]
    alerts_dict = alerts_df.to_dict(orient="list")

    data = file_path / 'data.json'
    data.write_text(json.dumps(alerts_dict))

    return alerts_dict, metadata_content
