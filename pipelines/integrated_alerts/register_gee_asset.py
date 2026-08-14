import json
import os
from datetime import datetime, timezone

import ee
from google.auth.transport.requests import AuthorizedSession

from pipelines.utils import get_secret

# Per-project service account + AWS Secrets Manager secret (holding that
# service account's key JSON) used to register a COG-backed GEE asset.
PROJECT_CONFIG = {
    "forma-250": {
        "service_account": "gfw-gee-export@forma-250.iam.gserviceaccount.com",
        "secret_id": "gcs-auth/forma-250",
    },
    "landandcarbon": {
        "service_account": "integrated-alerts@landandcarbon.iam.gserviceaccount.com",
        "secret_id": "gcs-auth/landandcarbon",
    },
}


def register_gee_asset(
    gcs_uri,
    gee_asset_path,
    project="forma-250",
    force=False,
    start_time="2015-01-01T00:00:00.000000000Z",
    end_time=None,
) -> str:
    """Register a COG-backed GEE image asset from a GCS uri.

    gcs_uri is the gs:// path to the source COG; gee_asset_path is the
    asset's path within the GEE project (without the projects/.../assets/
    prefix). project selects forma-250 or landandcarbon (mirroring gee.py's
    -l flag); force overwrites an existing asset at the same path (mirroring
    gee.py's -f flag). end_time defaults to today (UTC) if not given, since
    this asset represents a continuously updated dataset.
    """
    if project not in PROJECT_CONFIG:
        raise ValueError(
            f"Unknown project {project!r}; expected one of {sorted(PROJECT_CONFIG)}"
        )
    config = PROJECT_CONFIG[project]

    if end_time is None:
        end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00.000000000Z")

    gcs_path, gcs_file_name = os.path.split(gcs_uri)
    asset_path = f"projects/{project}/assets/{gee_asset_path}"

    url = f"https://earthengine.googleapis.com/v1alpha/projects/{project}/image:importExternal"
    request = {
        "imageManifest": {
            "name": asset_path,
            "uriPrefix": f"{gcs_path}/",
            "tilesets": [
                {"id": "0", "sources": [{"uris": [gcs_file_name]}]},
            ],
            "bands": [
                {
                    "id": "date_conf",
                    "tilesetId": "0",
                    "pyramidingPolicy": "MODE",
                    "missingData": {"values": [0]},
                },
            ],
            "startTime": start_time,
            "endTime": end_time,
        },
        "overwrite": force,
    }

    credentials = ee.ServiceAccountCredentials(
        config["service_account"], key_data=get_secret(config["secret_id"])
    )
    ee.Initialize(credentials, project=project)

    session = AuthorizedSession(credentials.with_quota_project(project))
    response = session.post(url=url, data=json.dumps(request))
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to register GEE asset {asset_path}: "
            f"{response.status_code} {json.loads(response.content)}"
        )

    print("Successfully registered")
    ee.data.setAssetAcl(asset_path, {"all_users_can_read": True})
    print("Set asset to be readable by everyone")
    print("Resulting ACL:", ee.data.getAssetAcl(asset_path))

    return asset_path
