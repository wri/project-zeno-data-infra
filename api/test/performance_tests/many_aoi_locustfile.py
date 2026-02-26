import os
import random
import time
from contextlib import contextmanager
from time import sleep

import requests
from locust import FastHttpUser, between, events, task

API_KEY = os.environ["API_KEY"]


@contextmanager
def measure_task_time(name):
    start_time = time.perf_counter()
    exception = None
    try:
        yield
    except Exception as e:
        exception = e
        raise
    finally:
        total_time_ms = (time.perf_counter() - start_time) * 1000
        events.request.fire(
            request_type="TASK",
            name=name,
            response_time=total_time_ms,
            response_length=0,
            exception=exception,
        )


class IntenseApiUser(FastHttpUser):
    wait_time = between(30, 60)

    @task
    def test_request_analytics_and_wait_for_result(self):
        aoi_type = random.choices(
            ["admin", "key_biodiversity_area", "protected_area", "indigenous_land"],
            weights=[4, 1, 1, 1],
        )[0]
        endpoint = random.choice(
            [
                "tree_cover",
                "tree_cover_loss",
                "tree_cover_gain",
                "grasslands",
                "natural_lands",
                "land_cover_change",
                "land_cover_composition",
                "dist_alerts",
            ]
        )
        print(f"{aoi_type} {endpoint}")

        if aoi_type == "admin":
            run_id = "precalc"
        else:
            run_id = "otf"

        with measure_task_time(f"full_run_{run_id}"):

            def _get_aoi_ids(aoi_type):
                max_val = 50 if aoi_type == "admin" else 25
                number_of_ids = random.choices(
                    range(1, max_val), weights=range(max_val, 1, -1)
                )[0]

                offset = random.randint(1, 10000)

                if aoi_type == "admin":
                    resp = requests.get(
                        f"https://data-api.globalforestwatch.org/dataset/gadm_administrative_boundaries/latest/query/json?sql=SELECT gid_2  from data where gid_2 is not null limit {number_of_ids} offset {offset}&x-api-key={API_KEY}"
                    ).json()
                    aoi_ids = [
                        row["gid_2"].removesuffix("_1").removesuffix("_2")
                        for row in resp["data"]
                    ]
                elif aoi_type == "key_biodiversity_area":
                    resp = requests.get(
                        f"https://data-api.globalforestwatch.org/dataset/birdlife_key_biodiversity_areas/latest/query?sql=select sitrecid from data where sitlat < 70 and sitlat > -70 limit {number_of_ids} offset {offset}&x-api-key={API_KEY}"
                    ).json()
                    aoi_ids = [str(row["sitrecid"]) for row in resp["data"]]
                elif aoi_type == "protected_area":
                    resp = requests.get(
                        f"https://data-api.globalforestwatch.org/dataset/wdpa_protected_areas/latest/query?sql=select wdpaid from data limit {number_of_ids} offset {offset}&x-api-key={API_KEY}"
                    ).json()
                    aoi_ids = [str(row["wdpaid"]) for row in resp["data"]]
                elif aoi_type == "indigenous_land":
                    resp = requests.get(
                        f"https://data-api.globalforestwatch.org/dataset/landmark_ip_lc_and_indicative_poly/latest/query?sql=select landmark_id from data limit {number_of_ids} offset {offset}&x-api-key={API_KEY}"
                    ).json()
                    aoi_ids = [str(row["landmark_id"]) for row in resp["data"]]
                return aoi_ids

            aoi_ids = _get_aoi_ids(aoi_type)

            analytics_in = {
                "aoi": {"type": aoi_type, "ids": aoi_ids},
            }

            if endpoint == "tree_cover" or endpoint == "tree_cover_loss":
                analytics_in["canopy_cover"] = 30
                analytics_in["forest_filter"] = None

                if endpoint == "tree_cover_loss":
                    analytics_in["start_year"] = "2001"
                    analytics_in["end_year"] = "2024"
                    analytics_in["intersections"] = []
            elif endpoint == "tree_cover_gain":
                analytics_in["forest_filter"] = None
                analytics_in["start_year"] = "2000"
                analytics_in["end_year"] = "2020"
            elif endpoint == "grasslands":
                analytics_in["start_year"] = "2015"
                analytics_in["end_year"] = "2022"
            elif endpoint == "carbon_flux":
                analytics_in["canopy_cover"] = 30
            elif endpoint == "dist_alerts":
                analytics_in["start_date"] = "2024-01-01"
                analytics_in["end_date"] = "2025-01-01"
                analytics_in["intersections"] = random.choice(
                    [[], ["driver"], ["natural_lands"], ["grasslands"], ["land_cover"]]
                )

            start_time = time.perf_counter()

            with self.rest(
                "POST", f"/v0/land_change/{endpoint}/analytics", json=analytics_in
            ) as post_response:
                if post_response.status_code != 202:
                    post_response.failure(
                        f"Got {post_response.status_code} instead of 202 for {endpoint} with body {analytics_in}"
                    )
                    return

            # request the results
            link = post_response.json()["data"]["link"]
            resource_id = link.split("/")[-1]
            max_retries = 1000
            retry_count = 0
            group_name = f"land_change:analytics::{aoi_type}::{endpoint}::resource"
            print(link)

            while retry_count < max_retries:
                with self.rest(
                    "GET",
                    f"/v0/land_change/{endpoint}/analytics/{resource_id}",
                    name=group_name,
                ) as response:
                    if "Retry-After" in response.headers:
                        retry_count = retry_count + 1

                        if retry_count >= max_retries:
                            response.failure(
                                f"Max retries exceeded for {endpoint} resource: {resource_id}"
                            )
                            break

                        try:
                            wait_time = float(response.headers["Retry-After"])
                            sleep(wait_time)
                        except ValueError:
                            response.failure(
                                f"Invalid Retry-After value for {endpoint} resource: {resource_id}"
                            )
                    else:
                        if response.status_code == 200:
                            events.request.fire(
                                request_type="CUSTOM",
                                name=f"processing_time_{aoi_type}_{endpoint}",
                                response_time=(time.perf_counter() - start_time) * 1000,
                                response_length=0,
                                exception=None,
                            )
                            response.success()
                        else:
                            response.failure(
                                f"Got {response.status_code} instead of 200 for {endpoint} resource: {resource_id}"
                            )

                        break
