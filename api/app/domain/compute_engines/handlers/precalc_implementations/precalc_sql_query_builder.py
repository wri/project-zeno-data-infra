from typing import List

from app.analysis.common.analysis import get_sql_in_list
from app.domain.models.dataset import DatasetQuery


class PrecalcSqlQueryBuilder:
    def build(self, aoi_ids, query: DatasetQuery) -> str:
        _aggregation_clause: str = str(query.aggregate)

        groupby_fields: List[str] = [
            "aoi_id",
            "aoi_type",
            *[dataset.get_field_name() for dataset in query.group_bys],
        ]

        groupby_clause: str = " GROUP BY " + ", ".join(groupby_fields)

        select_clause: str = (
            "SELECT " + ", ".join(groupby_fields) + ", " + _aggregation_clause
        )

        filter_clause = (
            " WHERE "
            + " AND ".join([str(filt) for filt in query.filters])
            + f" AND aoi_id in {get_sql_in_list(aoi_ids)}"
        )

        sql: str = select_clause + " FROM data_source" + filter_clause + groupby_clause
        return sql
