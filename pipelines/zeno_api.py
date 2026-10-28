from dataclasses import dataclass
from typing import Optional

import httpx

ZENO_API_URL = "https://api.globalnaturewatch.org"


@dataclass
class Subscription:
    """One AOI dataset subscription of a dashboard, as returned by the
    project-zeno API."""

    subscription_id: str
    dashboard_id: str
    user_id: str
    user_email: str
    aoi_type: str  # "admin", "protected_area", "key_biodiversity_area", ...
    aoi_id: str  # e.g. "BRA.16" for an admin AOI
    dataset: str  # "integrated_alerts"


class ZenoApiClient:
    """Minimal client for the project-zeno (Global Nature Watch) API.

    Requests are authenticated with a superuser key, sent as a bearer token. Any
    non-2xx response raises httpx.HTTPStatusError.
    """

    def __init__(
        self,
        token: str,
        base_url: str = ZENO_API_URL,
        http_client: Optional[httpx.Client] = None,
    ):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {token}"}
        self.http_client = http_client or httpx.Client(timeout=60)

    def list_subscriptions(self) -> list[Subscription]:
        """Return all active dashboard subscriptions."""
        response = self.http_client.get(
            f"{self.base_url}/api/admin/subscriptions", headers=self.headers
        )
        response.raise_for_status()
        return [
            Subscription(
                subscription_id=item["subscription_id"],
                dashboard_id=item["dashboard_id"],
                user_id=item["user_id"],
                user_email=item["user_email"],
                aoi_type=item["aoi_type"],
                aoi_id=item["aoi_id"],
                dataset=item["dataset"],
            )
            for item in response.json()
        ]
