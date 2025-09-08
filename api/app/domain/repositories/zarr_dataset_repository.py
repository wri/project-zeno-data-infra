import ast

import xarray as xr
from app.domain.models.dataset import Dataset
from shapely import Geometry
from shapely.geometry import mapping


class ZarrDatasetRepository:
    ZARR_LOCATIONS = {
        Dataset.area_hectares: "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr",
        Dataset.tree_cover_loss: "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/raster/epsg-4326/zarr/year.zarr",
        Dataset.tree_cover_gain: "s3://gfw-data-lake/umd_tree_cover_gain_from_height/v20240126/raster/epsg-4326/zarr/period.zarr",
        Dataset.canopy_cover: "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/zarr/threshold.zarr",
        Dataset.primary_forest: "s3://gfw-data-lake/umd_regional_primary_forest_2001/v201901/raster/epsg-4326/zarr/is.zarr",
        Dataset.intact_forest: "s3://gfw-data-lake/ifl_intact_forest_landscapes_2020/v2021/raster/epsg-4326/zarr/is.zarr"
    }

    def load(self, dataset: Dataset, geometry: Geometry = None) -> xr.DataArray:
        xarr = xr.open_zarr(
            self.ZARR_LOCATIONS[dataset],
            storage_options={"requester_pays": True},
        ).band_data
        xarr.rio.write_crs("EPSG:4326", inplace=True)
        xarr.name = dataset.get_field_name()

        if geometry is not None:
            return self._clip_xarr_to_geometry(xarr, geometry)
        return xarr

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
            value_tuple = ast.literal_eval(value)
            val_map = {"2000-2005": 1, "2005-2010": 2, "2010-2015": 3, "2015-2020": 4}

            return [val_map[val] for val in value_tuple]
        elif dataset == Dataset.primary_forest:
            return int(value)
        elif dataset == Dataset.intact_forest:
            return int(value)
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

        else:
            return series

    def _clip_xarr_to_geometry(self, xarr, geom):
        sliced = xarr.sel(
            x=slice(geom.bounds[0], geom.bounds[2]),
            y=slice(geom.bounds[3], geom.bounds[1]),
        )
        if "band" in sliced.dims:
            sliced = sliced.squeeze("band")

        geojson = mapping(geom)
        clipped = sliced.rio.clip([geojson])
        return clipped
