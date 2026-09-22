"""HTTP routes. Each module here defines one router, and ``base_router`` collects them (as in Rockwell).

Keep routes thin: validate the request, call a function in ``src.handler``, shape the response.
"""

from fastapi import APIRouter

from .health_service import health_router

base_router = APIRouter()
base_router.include_router(health_router)
