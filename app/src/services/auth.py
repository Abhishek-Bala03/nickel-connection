"""Service-to-service authentication: every route but the probes needs ``Authorization: Bearer <token>``.

The connection service is internal. The Nickel API layer is the only external surface, and it and the
scheduler call this service with the token Vault hands both sides (``NICKEL_SERVICE_TOKEN``, or the
file ``NICKEL_SERVICE_TOKEN_FILE`` names; read once by ``src.config``). Every router but the probes
takes :data:`ServiceToken` (``src.services``); ``/health`` and ``/ready`` stay open because K3s and
Docker call them with no credentials.

This dependency is the application-level check. The target is mTLS between services plus a K3s
NetworkPolicy that lets only the scheduler and the API layer reach this pod (both infra-repo work):
defence in depth, so the token is never the only wall. The comparison is constant-time; the presented
value is never logged or echoed, and a missing token and a wrong one get the same answer.
"""

import hmac
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.error import UnauthorizedError, handle_service_error

# A token this short is a guess, not a secret: python -c "import secrets; print(secrets.token_urlsafe(32))"
MIN_TOKEN_CHARS = 32
# auto_error=False: a missing or non-Bearer header comes back as None and is refused below, in the
# service's error shape, instead of FastAPI's own 403 or 401. Declared this way, the API docs show the
# scheme and take the token from their Authorize button.
_bearer = HTTPBearer(
    auto_error=False,
    scheme_name="ServiceToken",
    description="The service token (NICKEL_SERVICE_TOKEN); every route but /health and /ready.",
)


def check_token(token: str) -> None:
    """Refuse to start without a usable token: no token would mean no wall at all (fail closed)."""
    if not token:
        raise ValueError(
            "NICKEL_SERVICE_TOKEN (or NICKEL_SERVICE_TOKEN_FILE) is not set: the service does "
            "not start without its token (see .env.example)"
        )
    if len(token) < MIN_TOKEN_CHARS:
        raise ValueError(
            f"NICKEL_SERVICE_TOKEN is shorter than {MIN_TOKEN_CHARS} characters; use a random "
            "one (python -c 'import secrets; print(secrets.token_urlsafe(32))')"
        )


def _token_matches(presented: str, expected: str) -> bool:
    # compare_digest takes the same time whatever the first differing byte; only the length of the
    # presented value shows, and the length of a random token is no secret.
    return hmac.compare_digest(presented.encode(), expected.encode())


def require_service_token(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> None:
    expected = request.app.state.settings.service_token
    presented = credentials.credentials if credentials is not None else ""
    if not _token_matches(presented, expected):
        raise UnauthorizedError()


def _bearer_value(scope) -> str:
    """The token of a ``Bearer`` Authorization header in an ASGI scope, or ""."""
    for name, value in scope.get("headers", ()):
        if name == b"authorization":
            scheme, _, token = value.decode("latin-1").partition(" ")
            return token.strip() if scheme.lower() == "bearer" else ""
    return ""


class ServiceTokenMiddleware:
    """Refuse an unauthenticated request before routing, so nothing about the API shows without a token.

    The route dependency alone runs after FastAPI has read and validated the body: a malformed body
    got a 422 describing the request's shape, and an unknown path a 404, to a caller with no token
    (the kind of answer a Burp scan maps the API with). Here every request but the probes, and the
    docs pages where docs are on (development only), is checked first and gets the same 401 whatever
    its path, method or body. The dependency stays as the second check on every router.
    """

    PROBES = frozenset({"/health", "/ready"})

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or self._open(scope):
            await self.app(scope, receive, send)
            return
        expected = scope["app"].state.settings.service_token
        if _token_matches(_bearer_value(scope), expected):
            await self.app(scope, receive, send)
            return
        response = handle_service_error(Request(scope), UnauthorizedError())
        await response(scope, receive, send)

    def _open(self, scope) -> bool:
        app = scope["app"]
        docs = {
            app.docs_url,
            app.redoc_url,
            app.openapi_url,
            app.swagger_ui_oauth2_redirect_url,
        }
        return scope["path"] in self.PROBES or (
            app.docs_url is not None and scope["path"] in docs
        )


ServiceToken = Depends(require_service_token)
