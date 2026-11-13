from typing import Annotated, ClassVar

from pydantic import Field, model_validator

from app.models.common.base import StrictBaseModel
from app.models.land_change.tree_cover_loss import (
    DATE_REGEX,
    AllowedIntersections,
    AoiUnion,
    TreeCoverLossAnalyticsIn,
)


class ForestLossRequest(StrictBaseModel):
    """Tree cover loss in a forest chosen for the caller, from the first year
    that forest's map supports."""

    earliest_year: ClassVar[int]
    canopy_cover: ClassVar[int]

    aoi: Annotated[AoiUnion, Field(discriminator="type")] = Field(
        ..., title="AOI", description="AOI to calculate in."
    )
    start_year: str = Field(..., title="Start Year", pattern=DATE_REGEX)
    end_year: str = Field(..., title="End Year", pattern=DATE_REGEX)
    intersections: AllowedIntersections = Field(
        default_factory=list,
        max_length=1,
        description=(
            "Optional: driver returns loss by dominant driver, totalled over the "
            "period; fire returns fire-driven and other loss by year."
        ),
    )

    @model_validator(mode="after")
    def validate_years(self):
        if int(self.start_year) < self.earliest_year:
            raise ValueError(f"start_year must be {self.earliest_year} or later.")
        if int(self.end_year) < int(self.start_year):
            raise ValueError("end_year must be greater than or equal to start_year")
        return self

    def loss_in(self, forest_filter: str) -> TreeCoverLossAnalyticsIn:
        return TreeCoverLossAnalyticsIn(
            aoi=self.aoi,
            start_year=self.start_year,
            end_year=self.end_year,
            canopy_cover=self.canopy_cover,
            forest_filter=forest_filter,
            intersections=self.intersections,
        )


class DeforestationAnalyticsIn(ForestLossRequest):
    earliest_year: ClassVar[int] = 2002
    canopy_cover: ClassVar[int] = 30

    start_year: str = Field(
        ...,
        title="Start Year",
        description="Year in YYYY format, 2002 or later.",
        pattern=DATE_REGEX,
    )

    def tree_cover_loss_request(self, forest_filter: str) -> TreeCoverLossAnalyticsIn:
        return self.loss_in(forest_filter)


class EudrSupplyChainDeforestationAnalyticsIn(ForestLossRequest):
    earliest_year: ClassVar[int] = 2021
    canopy_cover: ClassVar[int] = 10

    start_year: str = Field(
        ...,
        title="Start Year",
        description=(
            "Year in YYYY format, 2021 or later: natural forest is mapped as of "
            "2020."
        ),
        pattern=DATE_REGEX,
    )

    def tree_cover_loss_request(self) -> TreeCoverLossAnalyticsIn:
        return self.loss_in("natural_forest_only")
