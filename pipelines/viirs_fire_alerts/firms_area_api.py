"""NASA FIRMS area API: worldwide active fire alerts for any recent days."""

import csv
import io
from datetime import date

import httpx


class FirmsAreaApi:
    """Alerts from one FIRMS source, such as VIIRS_NOAA20_NRT."""

    def __init__(self, client: httpx.Client, map_key: str, source: str):
        self.client = client
        self.map_key = map_key
        self.source = source

    def first_available_date(self) -> date:
        response = self.get(f"data_availability/csv/{self.map_key}/{self.source}")
        row = next(csv.DictReader(io.StringIO(response.text)))
        return date.fromisoformat(row["min_date"])

    def download(self, start: date, days: int) -> bytes:
        """CSV of alerts from start for this many days (at most 5)."""
        path = f"area/csv/{self.map_key}/{self.source}/world/{days}/{start}"
        return self.get(path).content

    def get(self, path: str) -> httpx.Response:
        response = self.client.get(f"https://firms.modaps.eosdis.nasa.gov/api/{path}")
        response.raise_for_status()
        return response
