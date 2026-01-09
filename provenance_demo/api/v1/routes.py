from fastapi import APIRouter

from .explain_ap import explain_ap
from .health import health_check

router = APIRouter(
    prefix="/api/v1",
    tags=["v1"],
)

router.add_api_route("/explainAp", explain_ap, methods=["POST"])
router.add_api_route("/health", health_check, methods=["GET"])
