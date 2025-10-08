from abc import ABC, abstractmethod


class ComputeService(ABC):
    @abstractmethod
    async def compute(self, payload: dict):
        pass
