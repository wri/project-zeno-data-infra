from typing import Optional

import rioxarray
import xarray as xr
from shapely import Geometry
from shapely.geometry import mapping

from app.domain.models.dataset import Dataset


class ZarrDatasetRepository:
    ZARR_LOCATIONS = {
        Dataset.area_hectares: "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr",
        Dataset.tree_cover_loss: "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/raster/epsg-4326/zarr/year.zarr",
        Dataset.tree_cover_gain: "s3://gfw-data-lake/umd_tree_cover_gain_from_height/v20240126/raster/epsg-4326/zarr/period.zarr",
        Dataset.canopy_cover: "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/zarr/threshold.zarr",
        Dataset.primary_forest: "s3://gfw-data-lake/umd_regional_primary_forest_2001/v201901/raster/epsg-4326/zarr/is.zarr",
        Dataset.intact_forest: "s3://gfw-data-lake/ifl_intact_forest_landscapes_2000/v2021/raster/epsg-4326/zarr/is.zarr",
        Dataset.carbon_emissions: "s3://gfw-data-lake/gfw_forest_carbon_gross_emissions/v20250430/raster/epsg-4326/zarr/Mg_CO2e.zarr",
        Dataset.tree_cover_loss_drivers: "s3://gfw-data-lake/wri_google_tree_cover_loss_drivers/v1.12/raster/epsg-4326/zarr/category.zarr",
        Dataset.natural_lands: "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr",
        Dataset.natural_forests: "s3://gfw-data-lake/sbtn_natural_forests_map/v202504/raster/epsg-4326/zarr/class.zarr",
    }

    def load(
        self, dataset: Dataset, geometry: Optional[Geometry] = None
    ) -> xr.DataArray:
        xarr = self.open_source(dataset)
        xarr.rio.write_crs("EPSG:4326", inplace=True)
        xarr.name = dataset.get_field_name()

        if geometry is not None:
            return self._clip_xarr_to_geometry(xarr, geometry)
        return xarr

    def open_source(self, dataset):
        return xr.open_zarr(
            self.ZARR_LOCATIONS[dataset],
            storage_options={"requester_pays": True},
        ).band_data

    def translate(self, dataset, value):
        """
        Translate a value to the pixel value in the dataset
        """
        if dataset == Dataset.canopy_cover:
            match value:
                case 0:
                    return 0
                case 10:
                    return 1
                case 15:
                    return 2
                case 20:
                    return 3
                case 25:
                    return 4
                case 30:
                    return 5
                case 50:
                    return 6
                case 75:
                    return 7
        elif dataset == Dataset.tree_cover_loss:
            return int(value) - 2000
        elif dataset == Dataset.tree_cover_gain:
            val_map = {"2000-2005": 1, "2005-2010": 2, "2010-2015": 3, "2015-2020": 4}
            return [val_map[val] for val in value]
        elif dataset == Dataset.primary_forest:
            return int(value)
        elif dataset == Dataset.natural_lands:
            return value
        elif dataset == Dataset.natural_forests:
            match value:
                case "Unknown":
                    return 0
                case "Natural Forest":
                    return 1
                case "Non-Natural Forest":
                    return 2
        elif dataset == Dataset.tree_cover_loss_drivers:
            match value:
                case "Unknown":
                    return 0
                case "Permanent agriculture":
                    return 1
                case "Hard commodities":
                    return 2
                case "Shifting cultivation":
                    return 3
                case "Logging":
                    return 4
                case "Wildfire":
                    return 5
                case "Settlements & Infrastructure":
                    return 6
                case "Other natural disturbances":
                    return 7
        else:
            raise NotImplementedError()

    def unpack(self, dataset, series):
        """
        Convert Zarr pixel values to actual pixel meaning for dataset
        """
        if dataset == Dataset.tree_cover_loss:
            return series + 2000
        elif dataset == Dataset.tree_cover_gain:

            def pixel_to_gain(val):
                match val:
                    case 0:
                        return ""
                    case 1:
                        return "2000-2005"
                    case 2:
                        return "2005-2010"
                    case 3:
                        return "2010-2015"
                    case 4:
                        return "2015-2020"

            return series.map(pixel_to_gain)
        elif dataset == Dataset.tree_cover_loss_drivers:
            drivers = {
                0: "Unknown",
                1: "Permanent agriculture",
                2: "Hard commodities",
                3: "Shifting cultivation",
                4: "Logging",
                5: "Wildfire",
                6: "Settlements & Infrastructure",
                7: "Other natural disturbances",
            }

            return series.map(lambda pixel: drivers[pixel])
        elif dataset == Dataset.natural_forests:
            natural_forests_class = {
                0: "Unknown",
                1: "Natural Forest",
                2: "Non-Natural Forest",
            }

            return series.map(lambda pixel: natural_forests_class[pixel])
        else:
            return series

    def _clip_xarr_to_geometry(self, xarr, geom):
        sliced = xarr.sel(
            x=slice(geom.bounds[0], geom.bounds[2]),
            y=slice(geom.bounds[3], geom.bounds[1]),
        )
        if "band" in sliced.dims:
            sliced = sliced.squeeze("band")

        # Exit early if the geometry is fully out of bounds of the dataset
        return sliced
