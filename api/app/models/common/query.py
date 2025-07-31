from typing import Any, List, Literal

from pydantic import BaseModel, Field


class Filter(BaseModel):
    field: str = Field(..., description="Field to filter on.")
    op: Literal["=", ">", "<", ">=", "<=", "in"] = Field(
        ..., description="Operation: =, >, <, >=, <=, or in."
    )
    value: Any = Field(..., description="Value to compare the field against.")


class Query(BaseModel):
    dataset: str = Field(..., description="Name of the dataset or table to query.")
    group_by: List[str] = Field(default=[], description="Fields to group by.")
    filters: List[Filter] = Field(default=[], description="List of filter conditions.")

    class Config:
        schema_extra = {
            "example": {
                "dataset": "tree_cover_data",
                "group_by": ["country", "year"],
                "filters": [
                    {"field": "country", "op": "=", "value": "BRA"},
                    {"field": "year", "op": ">=", "value": 2020},
                    {"field": "canopy_cover", "op": "=", "value": 30},
                ],
            }
        }
