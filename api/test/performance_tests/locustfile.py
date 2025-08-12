from locust import HttpUser, between, task


class ApiUser(HttpUser):
    wait_time = between(0.5, 2.5)

    @task
    def test_endpoint(self):
        self.client.get("/")
