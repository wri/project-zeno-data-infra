import posixpath
import shutil
import urllib.request
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Callable

import fsspec


def download_wikimedia_importance(url: str, path: str, user_agent: str) -> str:
    """Downloads Nominatim's Wikimedia importance file: one row per Wikipedia
    article or redirect, with its language, title, importance and Wikidata id
    (https://nominatim.org/2024/08/07/wikimedia-file.html).

    Returns the file's Last-Modified header, since the file has no version."""
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(request, timeout=900) as response:
        with open(path, "wb") as file:
            shutil.copyfileobj(response, file)
        return response.headers["Last-Modified"]


@dataclass(frozen=True)
class CachedWikimediaImportance:
    """A cached copy, and the date its source was last modified, which stands
    in for a version."""

    cache_uri: str
    source_date: str


class WikimediaImportanceCache:
    """Nominatim's Wikimedia importance file, cached in a folder per date its
    source was last modified. Its importance changes slowly, so the newest
    cached copy is used unless a refresh is asked for."""

    def __init__(
        self,
        url: str,
        cache_prefix: str,
        download: Callable[[str, str], str],
        file_name: str = "wikimedia-importance.csv.gz",
    ):
        self.url = url
        self.cache_prefix = cache_prefix
        self.download = download
        self.file_name = file_name

    def fetch(self, destination: str, refresh: bool) -> CachedWikimediaImportance:
        """Copies the file to `destination`, downloading it first if there's
        no cached copy or a refresh is asked for."""
        cache, prefix = fsspec.core.url_to_fs(self.cache_prefix)
        source_dates = sorted(
            posixpath.basename(folder) for folder in cache.glob(f"{prefix}/*")
        )
        if refresh or not source_dates:
            return self._download_and_cache(cache, prefix, destination)
        cached = self._cached(source_dates[-1])
        cache.get_file(f"{prefix}/{source_dates[-1]}/{self.file_name}", destination)
        return cached

    def _download_and_cache(
        self, cache, prefix: str, destination: str
    ) -> CachedWikimediaImportance:
        last_modified = self.download(self.url, destination)
        source_date = f"{parsedate_to_datetime(last_modified):%Y%m%d}"
        cache.makedirs(f"{prefix}/{source_date}", exist_ok=True)
        cache.put_file(destination, f"{prefix}/{source_date}/{self.file_name}")
        return self._cached(source_date)

    def _cached(self, source_date: str) -> CachedWikimediaImportance:
        return CachedWikimediaImportance(
            f"{self.cache_prefix}/{source_date}/{self.file_name}", source_date
        )
