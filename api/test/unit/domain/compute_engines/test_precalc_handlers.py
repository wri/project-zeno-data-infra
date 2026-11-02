from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (
    TreeCoverPrecalcHandler,
)
from app.domain.models.dataset import Dataset, DatasetAggregate, DatasetQuery
from app.models.common.areas_of_interest import AdminAreaOfInterest


def handler() -> TreeCoverPrecalcHandler:
    return TreeCoverPrecalcHandler(
        precalc_query_builder=None,
        precalc_query_service=None,
        next_handler=None,
    )


def area_query() -> DatasetQuery:
    return DatasetQuery(
        aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
        group_bys=[],
        filters=[],
    )


def test_precomputed_admin_levels_are_handled():
    assert handler().should_handle(AdminAreaOfInterest(ids=["BRA.1.2"]), area_query())


def test_deeper_admin_levels_are_passed_on():
    aoi = AdminAreaOfInterest(ids=["BRA.1.2.3"])

    assert not handler().should_handle(aoi, area_query())
