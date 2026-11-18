from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class AlertPointsFile(ABC):
    """An alert points export file at a local path, written a page at a time.

    extension: the file name extension of this format.
    content_type: the media type the file is served with.
    """

    extension: str
    content_type: str

    @abstractmethod
    def __init__(self, path: Path):
        """Prepare to write the file at path."""

    @abstractmethod
    def append(self, points: pd.DataFrame) -> None:
        """Append a page of formatted points to the file."""

    @abstractmethod
    def close(self) -> None:
        """Finish the file so it is ready to upload."""
