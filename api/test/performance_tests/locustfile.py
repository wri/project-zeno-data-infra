import inspect
import json
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator

from locust import FastHttpUser, between, events, task


class JSONLogReader(Iterator[Dict[str, Any]]):
    """Read test data from JSON file using an iterator, returns objects as dicts"""

    def __init__(self, file, **kwargs):
        # Check if we need to resolve relative paths
        if isinstance(file, str) and not os.path.isabs(file):
            # Get caller's module path
            caller_frame = inspect.stack()[1]
            caller_module = inspect.getmodule(caller_frame[0])

            if caller_module and hasattr(caller_module, "__file__"):
                module_dir = os.path.dirname(os.path.abspath(caller_module.__file__))
                file = os.path.join(module_dir, file)

        # Open file if path string was provided
        if isinstance(file, str):
            self.file = open(file)
            self.data = json.load(self.file, **kwargs)
            self.file.close()  # Close immediately after loading
        else:
            # Already a file-like object
            self.data = json.load(file, **kwargs)

        # Validate data format
        if not isinstance(self.data, list):
            raise TypeError("JSON data must be a list of objects")

        self._iterator = iter(self.data)

    def __next__(self) -> Dict[str, Any]:
        try:
            return next(self._iterator)
        except StopIteration:
            if not self.data:
                raise  # Re-raise if data is empty
            # Reset iterator and restart
            self._iterator = iter(self.data)
            return next(self._iterator)

    def __iter__(self):
        return self


request_log_reader = JSONLogReader("zeno-analytics-requests.json")


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


class ApiUser(FastHttpUser):
    wait_time = between(0.5, 2.5)

    @task
    def test_request_analytics_and_wait_for_result(self):
        with measure_task_time("test_request_analytics_and_wait_for_result"):
            request_info = next(request_log_reader)
            dataset = request_info["event"][: -len("_analytics_request")]
            aois = request_info["js.analytics_in"]

            with self.rest(
                "POST", f"/v0/land_change/{dataset}/analytics", json=aois
            ) as post_response:
                if post_response.status_code != 202:
                    post_response.failure(
                        f"Got {post_response.status_code} instead of 202 for {dataset} with body {aois}"
                    )
                    return

            # request the results
            resource_id = request_info["js.resource_id"]
            max_retries = 10
            retry_count = 0
            group_name = f"land_change:analytics:{dataset}:resource"

            while retry_count < max_retries:
                with self.rest(
                    "GET",
                    f"/v0/land_change/{dataset}/analytics/{resource_id}",
                    name=group_name,
                ) as response:
                    if "Retry-After" in response.headers:
                        retry_count = retry_count + 1

                        if retry_count >= max_retries:
                            response.failure(
                                f"Max retries exceeded for {dataset} resource: {resource_id}"
                            )
                            break

                        try:
                            wait_time = float(response.headers["Retry-After"])
                            time.sleep(wait_time)
                        except ValueError:
                            response.failure(
                                f"Invalid Retry-After value for {dataset} resource: {resource_id}"
                            )
                    else:
                        if response.status_code == 200:
                            response.success()
                        else:
                            response.failure(
                                f"Got {response.status_code} instead of 200 for {dataset} resource: {resource_id}"
                            )

                        break
