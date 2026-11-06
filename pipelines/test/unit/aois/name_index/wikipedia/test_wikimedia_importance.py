from pathlib import Path

import pytest

from pipelines.aois.name_index.wikipedia.wikimedia_importance import (
    WikimediaImportanceCache,
)

URL = "https://nominatim.org/data/wikimedia-importance.csv.gz"


def cached(cache_prefix, source_date, content):
    path = Path(cache_prefix) / source_date / "wikimedia-importance.csv.gz"
    path.parent.mkdir(parents=True)
    path.write_text(content)


def no_download(url, destination):
    raise AssertionError("downloaded although a cached copy exists")


def test_the_newest_cached_copy_is_used_without_downloading(tmp_path):
    cache_prefix = str(tmp_path / "cache")
    cached(cache_prefix, "20250301", "older")
    cached(cache_prefix, "20251127", "newer")
    destination = tmp_path / "wikimedia.csv.gz"

    cache = WikimediaImportanceCache(URL, cache_prefix, no_download)
    copy = cache.fetch(str(destination), refresh=False)

    assert destination.read_text() == "newer"
    assert copy.source_date == "20251127"


@pytest.mark.parametrize("refresh", [True, False], ids=["refresh", "empty cache"])
def test_a_download_is_cached_under_its_source_date(tmp_path, refresh):
    cache_prefix = str(tmp_path / "cache")
    if refresh:
        cached(cache_prefix, "20250301", "older")
    destination = tmp_path / "wikimedia.csv.gz"

    def download(url, path):
        Path(path).write_text("latest")
        return "Thu, 27 Nov 2025 08:22:57 GMT"

    cache = WikimediaImportanceCache(URL, cache_prefix, download)
    copy = cache.fetch(str(destination), refresh=refresh)

    assert copy.source_date == "20251127"
    assert Path(copy.cache_uri).read_text() == "latest"
    assert destination.read_text() == "latest"
