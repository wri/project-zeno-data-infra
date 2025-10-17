from enum import Enum
from typing import Annotated, List, Optional, Union

from pydantic import Field, PrivateAttr, model_validator

from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
)
from app.models.common.base import Response, StrictBaseModel

ANALYTICS_NAME = "deforestation_emissions_by_crop"

AoiUnion = Union[AdminAreaOfInterest,]
DATE_REGEX = r"^\d{4}$"


class CropType(Enum):
    BANA = "Banana"
    BARL = "Barley"
    BEAN = "Bean"
    CASS = "Cassava"
    CHIC = "Chickpea"
    CNUT = "Coconut"
    COCO = "Cocoa"
    ACOF = "Arabica Coffee"
    RCOF = "Robusta Coffee"
    COTT = "Cotton"
    COWP = "Cowpea"
    GROU = "Groundnut"
    LENT = "Lentil"
    MAIZ = "Maize"
    PMIL = "Pearl Millet"
    SMIL = "Small Millet"
    OILP = "Oil Palm"
    PIGE = "Pigeon Pea"
    PLNT = "Plantain"
    POTA = "Potato"
    RAPE = "Rapeseed"
    RICE = "Rice"
    SESA = "Sesame Seed"
    SORG = "Sorghum"
    SOYB = "Soybean"
    SUGB = "Sugarbeet"
    SUGC = "Sugarcane"
    SUNF = "Sunflower"
    SWPO = "Sweet Potato"
    TEAS = "Tea"
    TOBA = "Tobacco"
    WHEA = "Wheat"
    YAMS = "Yams"
    OCER = "Other Cereals"
    OFIB = "Other Fibre Crops"
    OOIL = "Other Oil Crops"
    OPUL = "Other Pulses"
    ORTS = "Other Roots"
    REST = "Rest of Crops"
    TEMF = "Temperate Fruit"
    TROF = "Tropical Fruit"
    VEGE = "Vegetables"

    def __str__(self):
        return self.value


class GasType(Enum):
    co2e = "CO2e"
    co2 = "CO2"
    ch4 = "CH4"
    n2o = "N20"

    def __str__(self):
        return self.value


class DeforestationEmissionsByCropAnalyticsIn(AnalyticsIn):
    _analytics_name: str = PrivateAttr(default=ANALYTICS_NAME)
    _version: str = PrivateAttr(default="v0")
    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ...,
        title="AOI",
        description="AOI to calculate in.",
    )
    gas_types: List[GasType] = Field(
        title="Gas Types",
        description="Gas types to include. CO2e is equivalent to the sum of all other gases as equivalent to CO2.",
        default=[GasType.co2e],
    )
    crop_types: List[CropType] = Field(
        ..., title="Crop Types", description="Crop types to include."
    )
    start_year: str = Field(
        ...,
        title="Start Date",
        description="Must be year in YYYY date format. Minimum year is 2020.",
        pattern=DATE_REGEX,
        examples=["2020", "2024"],
    )
    end_year: str = Field(
        ...,
        title="End Date",
        description="Must be year in YYYY date format. Maximum year is 2024.",
        examples=["2020", "2024"],
        ge="2020",  # Minimum value
        le="2024",  # Maximum value (adjust as needed)
    )

    @model_validator(mode="after")
    def validate_year_range(self):
        start = int(self.start_year)
        end = int(self.end_year)
        if end < start:
            raise ValueError("end_year must be greater than or equal to start_year")
        return self


class DeforestationEmissionsByCropResult(StrictBaseModel):
    aoi_id: List[str]
    aoi_type: List[str]
    crop_type: List[str]
    gas_type: List[str]
    year: List[int]
    emissions_factor: List[float]
    emissions_tonnes: List[float]
    production_tonnes: List[float]


class DeforestationEmissionsByCropAnalytics(StrictBaseModel):
    result: Optional[DeforestationEmissionsByCropResult] = None
    metadata: Optional[dict] = None
    message: Optional[str] = None
    status: Optional[AnalysisStatus] = None

    model_config = {
        "from_attributes": True,
        "validate_by_name": True,
    }


class DeforestationEmissionsByCropAnalyticsResponse(Response):
    data: DeforestationEmissionsByCropAnalytics
