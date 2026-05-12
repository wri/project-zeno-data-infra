import json
import uuid
from abc import ABC, abstractmethod
from typing import Dict

from app.domain.models.analysis import Analysis


class Analyzer(ABC):
    input_uris: Dict[str, str] | None = None

    @abstractmethod
    async def analyze(self, analysis: Analysis) -> None:
        pass

    def thumbprint(self) -> uuid.UUID:
        if self.input_uris is None:
            raise RuntimeError("Input URIs must be provided for thumbprinting")

        return uuid.uuid5(
            uuid.NAMESPACE_DNS,
            json.dumps(self.input_uris),
        )
