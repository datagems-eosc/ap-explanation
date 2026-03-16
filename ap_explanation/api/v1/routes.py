from fastapi import APIRouter

from .health import health_check, readiness_check
from .provenance.managed import router as managed_router
from .provenance.manual import router as manual_router

router = APIRouter(prefix="/api/v1", tags=["aps"])

router.include_router(managed_router)
router.include_router(manual_router)

# Health check (liveness)
router.add_api_route("/health", health_check, methods=["GET"])
# Readiness check — verifies DB and Redis are reachable
router.add_api_route("/ready", readiness_check, methods=["GET"])
