from typing import Any, Optional

import httpx

SPARKPOST_URL = "https://api.sparkpost.com/api/v1"
# SparkPost's error code for a request with no valid recipient address.
INVALID_RECIPIENT_CODE = "5002"


class RejectedRecipientError(Exception):
    """SparkPost won't send to the recipients (e.g. an invalid address), so
    sending again won't help.

    Recipients SparkPost has suppressed (after earlier hard bounces, spam
    complaints or unsubscribes) don't raise this: SparkPost accepts those sends,
    and they bounce afterwards.
    """


def is_transient_error(error: BaseException) -> bool:
    """Whether a send that failed with this error may succeed if retried: rate
    limits, SparkPost server errors and network errors, but not errors in the
    request (e.g. a bad API key or template)."""
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        return status == 429 or status >= 500
    return isinstance(error, httpx.TransportError)


class SparkPostClient:
    """Minimal SparkPost client for sending emails from stored templates.
    Rejected recipients raise RejectedRecipientError, and any other non-2xx
    response raises httpx.HTTPStatusError.
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
        if response.status_code == 400:
            errors = _errors(response)
            # SparkPost's codes are numbers or strings, depending on the error.
            if any(str(e.get("code")) == INVALID_RECIPIENT_CODE for e in errors):
                raise RejectedRecipientError(
                    "; ".join(str(e.get("message")) for e in errors)
                )
        response.raise_for_status()
        results = response.json()["results"]
        if results.get("total_accepted_recipients") == 0:
            raise RejectedRecipientError(f"No recipients accepted: {results}")
        return results


def _errors(response: httpx.Response) -> list[dict]:
    try:
        return response.json().get("errors", [])
    except ValueError:
        return []
