from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class RequireBearerToken:
    """Rejects HTTP requests that carry no bearer token. The token itself is
    validated by the API it is forwarded to."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and not self.has_bearer_token(scope):
            response = JSONResponse({"detail": "Bearer token required"}, 401)
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)

    def has_bearer_token(self, scope: Scope) -> bool:
        for name, value in scope["headers"]:
            if name.lower() == b"authorization":
                return value.lower().startswith(b"bearer ")
        return False
