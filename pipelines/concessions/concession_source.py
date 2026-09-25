from dataclasses import dataclass


@dataclass(frozen=True)
class ConcessionSource:
    """Where a concession dataset lives in the data lake, and which of its
    (lowercased) attribute columns to leave out of the outputs."""

    concession_type: str
    source_uri: str
    dropped_columns: tuple[str, ...] = ()

    def uri_for(self, version: str) -> str:
        return self.source_uri.format(version=version)
