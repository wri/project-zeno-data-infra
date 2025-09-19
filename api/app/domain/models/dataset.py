from enum import Enum
from typing import Any, List, Literal

from app.models.common.base import StrictBaseModel


class Dataset(Enum):
    tree_cover_loss = "tree_cover_loss"
    tree_cover_gain = "tree_cover_gain"
    canopy_cover = "canopy_cover"
    area_hectares = "area_hectares"
    intact_forest = "intact_forest"
    primary_forest = "primary_forest"
    carbon_emissions = "carbon_emissions"
    tree_cover_loss_drivers = "tree_cover_loss_driver"

    def get_field_name(self):
        DATASET_TO_NAMES = {
            Dataset.area_hectares: "area_ha",
            Dataset.tree_cover_loss: "tree_cover_loss_year",
            Dataset.tree_cover_gain: "tree_cover_gain_period",
            Dataset.canopy_cover: "canopy_cover",
            Dataset.intact_forest: "is_intact_forest",
            Dataset.primary_forest: "is_primary_forest",
            Dataset.tree_cover_loss_drivers: "tree_cover_loss_driver",
            Dataset.carbon_emissions: "carbon_emissions_MgCO2e",
        }

        return DATASET_TO_NAMES[self]


class DatasetFilter(StrictBaseModel):
    dataset: Dataset
    op: Literal["=", "<", ">", "<=", ">=", "!=", "in"]
    value: Any

    def __str__(self):
        field = self.dataset.get_field_name()
        # Supress trailing commas of monotuples and surround (only) strings with single quotes
        match self.value:
            case (x,):
                try:
                    float(x)
                    int(x)
                    value_repr = f"({x})"
                except ValueError:
                    value_repr = f"('{x}')"
            case x:
                try:
                    float(x)
                    int(x)
                    value_repr = f"{x}"
                except ValueError:
                    value_repr = f"'{x}'"

        return " ".join([field, self.op, value_repr])


class DatasetAggregate(StrictBaseModel):
    datasets: List[Dataset]
    func: Literal["sum", "count"]

    def __str__(self):
        clauses: List[str] = [
            "".join(
                [
                    self.func.upper(),
                    f"({ds.get_field_name()})",
                    f" AS {ds.get_field_name()}",
                ]
            )
            for ds in self.datasets
        ]

        return ", ".join([clause for clause in clauses])


class DatasetQuery(StrictBaseModel):
    aggregate: DatasetAggregate
    group_bys: List[Dataset]
    filters: List[DatasetFilter]
