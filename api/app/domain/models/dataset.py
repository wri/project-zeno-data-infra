from enum import Enum
from typing import Any, List, Literal

from app.models.common.base import StrictBaseModel


class Dataset(Enum):
    area_hectares = "area_hectares"
    canopy_cover = "canopy_cover"
    carbon_emissions = "carbon_emissions"
    intact_forest = "intact_forest"
    natural_forests = "natural_forests"
    natural_lands = "natural_lands"
    primary_forest = "primary_forest"
    tree_cover_gain = "tree_cover_gain"
    tree_cover_loss = "tree_cover_loss"
    tree_cover_loss_drivers = "tree_cover_loss_driver"

    def get_field_name(self):
        DATASET_TO_NAMES = {
            Dataset.area_hectares: "area_ha",
            Dataset.canopy_cover: "canopy_cover",
            Dataset.carbon_emissions: "carbon_emissions_MgCO2e",
            Dataset.intact_forest: "is_intact_forest",
            Dataset.natural_forests: "natural_forests_class",
            Dataset.natural_lands: "natural_lands_class",
            Dataset.primary_forest: "is_primary_forest",
            Dataset.tree_cover_gain: "tree_cover_gain_period",
            Dataset.tree_cover_loss: "tree_cover_loss_year",
            Dataset.tree_cover_loss_drivers: "tree_cover_loss_driver",
        }

        return DATASET_TO_NAMES[self]


class DatasetFilter(StrictBaseModel):
    dataset: Dataset
    op: Literal["=", "<", ">", "<=", ">=", "!=", "in"]
    value: Any

    def __str__(self):
        field = self.dataset.get_field_name()
        # Supress trailing commas of monotuples
        match self.value:
            case (x,):
                value_repr: str = f"('{x}')"
            case x:
                value_repr = str(x)
        return " ".join([field, self.op, value_repr])


class DatasetAggregate(StrictBaseModel):
    datasets: List[Dataset]
    func: Literal["sum", "count"]


class DatasetQuery(StrictBaseModel):
    aggregate: DatasetAggregate
    group_bys: List[Dataset]
    filters: List[DatasetFilter]
