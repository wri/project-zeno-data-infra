from typing import Any, Optional

import httpx

SPARKPOST_URL = "https://api.sparkpost.com/api/v1"


class SparkPostClient:
    """Minimal SparkPost client for sending emails from stored templates.
    Any non-2xx response raises httpx.HTTPStatusError.
    """

    def __init__(
        self,
        api_key: str,
        track_links: bool = False,
        base_url: str = SPARKPOST_URL,
        http_client: Optional[httpx.Client] = None,
    ):
        self.base_url = base_url
        self.track_links = track_links
        # SparkPost takes the raw API key, with no "Bearer" prefix.
        self.headers = {"Authorization": api_key}
        self.http_client = http_client or httpx.Client(timeout=60)

    def send_template(
        self,
        template_id: str,
        substitution_data: dict[str, Any],
        recipients: list[str],
    ) -> dict:
        """Send the template filled in with substitution_data to each recipient
        email address, and return SparkPost's results (id,
        total_accepted_recipients and total_rejected_recipients)."""
        body: dict[str, Any] = {
            "content": {"template_id": template_id},
            "substitution_data": substitution_data,
            "recipients": [{"address": {"email": email}} for email in recipients],
        }
        # Without tracking turned off, the template's or account's settings apply.
        if not self.track_links:
            body["options"] = {"open_tracking": False, "click_tracking": False}
        response = self.http_client.post(
            f"{self.base_url}/transmissions", json=body, headers=self.headers
        )
        response.raise_for_status()
        return response.json()["results"]
