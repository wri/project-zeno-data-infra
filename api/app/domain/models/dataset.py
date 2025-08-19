from enum import Enum


class Dataset(Enum):
    tree_cover_loss = "tree_cover_loss"
    canopy_cover = "canopy_cover"
    area_hectares = "area_hectares"

    def get_field_name(self):
        DATASET_TO_NAMES = {
            Dataset.area_hectares: "area_ha",
            Dataset.tree_cover_loss: "loss_year",
            Dataset.canopy_cover: "canopy_cover",
        }

        return DATASET_TO_NAMES[self]
