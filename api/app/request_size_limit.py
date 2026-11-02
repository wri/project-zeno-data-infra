from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class RequestSizeLimit:
    """Rejects request bodies larger than max_bytes on paths under path_prefix
    with a 413, before they are read into memory: by Content-Length when sent,
    otherwise as the body streams in."""

    def __init__(self, app: ASGIApp, max_bytes: int, path_prefix: str):
        self.app = app
        self.max_bytes = max_bytes
        self.path_prefix = path_prefix

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not scope["path"].startswith(self.path_prefix):
            await self.app(scope, receive, send)
            return

        too_large = JSONResponse(
            {"detail": f"Request body is larger than {self.max_bytes} bytes"},
            status_code=413,
        )
        length = dict(scope["headers"]).get(b"content-length")
        if length is not None and int(length) > self.max_bytes:
            await too_large(scope, receive, send)
            return

        received = 0
        rejected = False

        async def limited_receive() -> Message:
            # Once over the limit, answer with the 413 and tell the app the client
            # disconnected, so it stops reading; its own error response is dropped.
            nonlocal received, rejected
            if rejected:
                return {"type": "http.disconnect"}
            message = await receive()
            received += len(message.get("body", b""))
            if received > self.max_bytes:
                rejected = True
                await too_large(scope, receive, send)
                return {"type": "http.disconnect"}
            return message

        async def send_unless_rejected(message: Message) -> None:
            if not rejected:
                await send(message)

        await self.app(scope, limited_receive, send_unless_rejected)
