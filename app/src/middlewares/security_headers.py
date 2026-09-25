"""Headers every response carries, so a browser or a scanner finds nothing to flag.

The service is an internal JSON API: nothing it returns should be sniffed as another content type,
framed, cached (a response can carry document metadata), or leak its URL as a referrer. A pure ASGI
middleware, so a streamed response is not buffered and even a 401 from the token check carries them.
No Content-Security-Policy: the API docs (development only) load Swagger UI's script and styles.
"""

HEADERS = (
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"cache-control", b"no-store"),
    (b"referrer-policy", b"no-referrer"),
)


class SecurityHeadersMiddleware:
    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message) -> None:
            if message["type"] == "http.response.start":
                present = {name.lower() for name, _ in message.get("headers", [])}
                extra = [
                    (name, value) for name, value in HEADERS if name not in present
                ]
                message = {**message, "headers": [*message.get("headers", []), *extra]}
            await send(message)

        await self.app(scope, receive, send_with_headers)
