"""Google S2 cells of alert points, for partitioning and ordering alert tables."""

import pyarrow as pa
import s2sphere


def add_s2_cells(alerts: pa.Table, coarse_level: int, fine_level: int) -> pa.Table:
    """Add s2_cell and s2_cell_fine columns: the S2 cells at these levels
    containing each alert's point, as signed 64-bit integers to fit Iceberg's
    long type."""
    fine_cells = [
        s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon)).parent(
            fine_level
        )
        for lat, lon in zip(
            alerts.column("latitude").to_pylist(),
            alerts.column("longitude").to_pylist(),
        )
    ]
    coarse_cells = [cell.parent(coarse_level) for cell in fine_cells]
    return alerts.append_column(
        "s2_cell", pa.array(signed_ids(coarse_cells), pa.int64())
    ).append_column("s2_cell_fine", pa.array(signed_ids(fine_cells), pa.int64()))


def signed_ids(cells) -> list:
    ids = [cell.id() for cell in cells]
    return [cell_id - 2**64 if cell_id >= 2**63 else cell_id for cell_id in ids]
