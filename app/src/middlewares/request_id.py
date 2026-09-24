"""Give every request an id, keep it on ``request.state.request_id`` and return it in ``X-Request-ID``.

A caller's own id is kept when it is short and plain (letters, digits, ``.``, ``_``, ``-``), so ids can
follow a request across services; anything else is replaced, which also keeps odd characters out of logs.
"""

import re
import uuid

from starlette.middleware.base import BaseHTTPMiddleware

HEADER = "X-Request-ID"
_PLAIN = re.compile(r"[A-Za-z0-9._-]{1,64}")


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        incoming = request.headers.get(HEADER, "")
        request_id = incoming if _PLAIN.fullmatch(incoming) else uuid.uuid4().hex
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers[HEADER] = request_id
        return response
