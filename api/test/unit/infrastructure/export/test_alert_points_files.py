from typing import get_args

from app.infrastructure.export.alert_points_files import ALERT_POINTS_FILE_TYPES
from app.models.land_change.integrated_alerts import IntegratedAlertsPointsExportIn


def test_every_export_format_has_a_file_type():
    formats = get_args(IntegratedAlertsPointsExportIn.model_fields["format"].annotation)

    assert set(ALERT_POINTS_FILE_TYPES) == set(formats)
