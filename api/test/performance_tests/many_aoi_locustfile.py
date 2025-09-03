import os
import random
import time
from time import sleep

import requests
from locust import FastHttpUser, between, events, task

API_KEY = os.environ["API_KEY"]


class IntenseApiUser(FastHttpUser):
    wait_time = between(0.5, 2.5)

    @task
    def test_request_analytics_and_wait_for_result(self):
        aoi_type = random.choice(["admin", "key_biodiversity_area"])

        def _get_aoi_ids(aoi_type):
            number_of_ids = random.randint(1, 50)
            offset = random.randint(1, 10000)

            if aoi_type == "admin":
                resp = requests.get(
                    f"https://data-api.globalforestwatch.org/dataset/gadm_administrative_boundaries/latest/query/json?sql=SELECT gid_2  from data where gid_2 is not null limit {number_of_ids} offset {offset}&x-api-key={API_KEY}"
                ).json()
                aoi_ids = [
                    row["gid_2"].removesuffix("_1").removesuffix("_2")
                    for row in resp["data"]
                ]
            else:
                resp = requests.get(
                    f"https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query?sql=select sitrecid from data limit {number_of_ids} offset {offset}&x-api-key={API_KEY}"
                ).json()
                aoi_ids = [str(row["sitrecid"]) for row in resp["data"]]

            return aoi_ids

        aoi_ids = _get_aoi_ids(aoi_type)

        analytics_in = {
            "aoi": {"type": aoi_type, "ids": aoi_ids},
            "start_year": "2001",
            "end_year": "2024",
            "canopy_cover": 30,
            "forest_filter": None,
            "intersections": [],
        }

        start_time = time.perf_counter()

        with self.rest(
            "POST", "/v0/land_change/tree_cover_loss/analytics", json=analytics_in
        ) as post_response:
            if post_response.status_code != 202:
                post_response.failure(
                    f"Got {post_response.status_code} instead of 202 for tree_cover_loss with body {analytics_in}"
                )
                return

        # request the results
        resource_id = post_response.json()["data"]["link"].split("/")[-1]
        max_retries = 50
        retry_count = 0
        group_name = "land_change:analytics::tree_cover_loss::resource"

        while retry_count < max_retries:
            with self.rest(
                "GET",
                f"/v0/land_change/tree_cover_loss/analytics/{resource_id}",
                name=group_name,
            ) as response:
                if "Retry-After" in response.headers:
                    retry_count = retry_count + 1

                    if retry_count >= max_retries:
                        response.failure(
                            f"Max retries exceeded for tree_cover_loss resource: {resource_id}"
                        )
                        break

                    try:
                        wait_time = float(response.headers["Retry-After"])
                        sleep(wait_time)
                    except ValueError:
                        response.failure(
                            f"Invalid Retry-After value for tree_cover_loss resource: {resource_id}"
                        )
                else:
                    if response.status_code == 200:
                        events.request.fire(
                            request_type="CUSTOM",
                            name="processing_time",
                            response_time=(time.perf_counter() - start_time) * 1000,
                            response_length=0,
                            exception=None,
                        )
                        response.success()
                    else:
                        response.failure(
                            f"Got {response.status_code} instead of 200 for tree_cover_loss resource: {resource_id}"
                        )

                    break
