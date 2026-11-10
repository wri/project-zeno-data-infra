import pyarrow as pa
import s2sphere

from pipelines.viirs_fire_alerts.alert_cells import add_s2_cells


def unsigned(cell: int) -> int:
    return cell + 2**64 if cell < 0 else cell


def test_add_s2_cells_gives_signed_coarse_cells_containing_the_fine_cells():
    # One point's S2 id is above 2**63, so it must wrap to fit a signed long.
    alerts = pa.table({"latitude": [-33.9, -8.1], "longitude": [151.2, -75.0]})

    cells = add_s2_cells(alerts, coarse_level=2, fine_level=13)

    coarse = cells.column("s2_cell").to_pylist()
    fine = cells.column("s2_cell_fine").to_pylist()
    assert cells.schema.field("s2_cell").type == pa.int64()
    assert min(coarse) < 0
    for coarse_cell, fine_cell in zip(coarse, fine):
        fine_id = s2sphere.CellId(unsigned(fine_cell))
        assert fine_id.level() == 13
        assert fine_id.parent(2).id() == unsigned(coarse_cell)
