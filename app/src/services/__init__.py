"""HTTP routes. Each module here defines one router, and ``base_router`` collects them (as in Rockwell).

Keep routes thin: validate the request, call a function in ``src.handler``, shape the response.
"""

from fastapi import APIRouter

from .auth import ServiceToken
from .connectors_service import connectors_router
from .contracts_service import contracts_router
from .documents_service import documents_router
from .health_service import health_router, version_router
from .jobs_service import jobs_router

base_router = APIRouter()
# The probes answer without a token: K3s and Docker call them with none. Every other router takes the
# service token (src.services.auth); a new router joins the second group, never the first.
base_router.include_router(health_router)
for router in (
    version_router,
    jobs_router,
    documents_router,
    connectors_router,
    contracts_router,
):
    base_router.include_router(router, dependencies=[ServiceToken])
