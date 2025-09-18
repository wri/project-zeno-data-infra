from app.domain.models.dataset import Dataset, DatasetQuery


class PrecalcSqlQueryBuilder:
    FIELDS = {
        Dataset.area_hectares: "area_ha",
        Dataset.tree_cover_loss: "tree_cover_loss_year",
        Dataset.tree_cover_gain: "tree_cover_gain_period",
        Dataset.canopy_cover: "canopy_cover",
        Dataset.intact_forest: "is_intact_forest",
        Dataset.primary_forest: "is_primary_forest",
        Dataset.carbon_emissions: "carbon_emissions_MgCO2e",
        Dataset.tree_cover_loss_drivers: "tree_cover_loss_driver",
    }

    def build(self, aoi_ids, query: DatasetQuery) -> str:
        agg = repr(query.aggregate)

        groupby_fields = (
            ", ".join([self.FIELDS[dataset] for dataset in query.group_bys])
            if len(query.group_bys) > 0
            else None
        )
        groupby_fields = f", {groupby_fields}" if groupby_fields else ""

        filters = " AND ".join([repr(filt) for filt in query.filters])

        filters += (
            f" AND aoi_id in ({', '.join([f"'{aoi_id}'" for aoi_id in aoi_ids])})"
        )

        sql = f"SELECT aoi_id, aoi_type{groupby_fields}, {agg} FROM data_source WHERE {filters} GROUP BY aoi_id, aoi_type{groupby_fields}"
        return sql
