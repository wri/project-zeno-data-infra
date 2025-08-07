from app.infrastructure.external_services.compute_service import ComputeService


class DataApiComputeService(ComputeService):

    async def compute(self, payload: dict):
        pass