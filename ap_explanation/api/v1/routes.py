from fastapi import APIRouter

from .health import health_check
from .provenance.managed import router as managed_router
from .provenance.manual import router as manual_router

router = APIRouter(prefix="/api/v1", tags=["v1"])

router.include_router(managed_router)
router.include_router(manual_router)

# Health check
router.add_api_route("/health", health_check, methods=["GET"])
