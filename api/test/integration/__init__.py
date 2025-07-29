import json
import os
import time
from pathlib import Path

import pytest


##################################################################
# Utility functions for managing test data                       #
# Since we're just beginning, I don't want to move these out,    #
# yet.                                                           #
##################################################################
def delete_resource_files(resource_id: str) -> Path:
    dir_path = Path(f"/tmp/dist_alerts_analytics_payloads/{resource_id}")

    if os.path.exists(dir_path):
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

    return dir_path


def write_metadata_file(dir_path):
    metadata_file = dir_path / "metadata.json"
    metadata_file.write_text(
        json.dumps(
            {
                "aoi": {
                    "ids": ["IDN.24.9"],
                    "provider": "gadm",
                    "type": "admin",
                    "version": "4.1",
                },
                "end_date": "2024-08-16",
                "intersections": [],
                "start_date": "2024-08-15",
            }
        )
    )


def write_data_file(dir_path, data):
    data_file = dir_path / "data.json"
    data_file.write_text(json.dumps(data))


async def retry_getting_resource(resource_id: str, client):
    resource = await client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}")
    data = resource.json()["data"]
    status = data["status"]
    attempts = 1
    while status == "pending" and attempts < 10:
        resp = await client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}")
        data = resp.json()["data"]
        status = data["status"]
        time.sleep(1)
        attempts += 1
    if attempts >= 10:
        pytest.fail("Resource stuck on 'pending' status")
    return data
