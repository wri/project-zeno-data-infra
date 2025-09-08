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


    def get_field_name(self):
        DATASET_TO_NAMES = {
            Dataset.area_hectares: "area_ha",
            Dataset.tree_cover_loss: "tree_cover_loss_year",
            Dataset.tree_cover_gain: "tree_cover_gain_period",
            Dataset.canopy_cover: "canopy_cover",
            Dataset.intact_forest: "is_intact_forest",
            Dataset.primary_forest: "is_primary_forest",
        }

        return DATASET_TO_NAMES[self]


class DatasetFilter(StrictBaseModel):
    dataset: Dataset
    op: Literal["=", "<", ">", "<=", ">=", "!=", "in"]
    value: Any


class DatasetAggregate(StrictBaseModel):
    dataset: Dataset
    func: Literal["sum", "count"]


class DatasetQuery(StrictBaseModel):
    aggregate: DatasetAggregate
    group_bys: List[Dataset]
    filters: List[DatasetFilter]
