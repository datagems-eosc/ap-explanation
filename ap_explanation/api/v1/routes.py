from fastapi import APIRouter

from .annotate import annotate_ap, annotate_ap_with_semiring, remove_annotation_ap
from .explain import explain_ap, explain_ap_with_semiring
from .health import health_check

router = APIRouter(
    prefix="/api/v1",
    tags=["v1"],
)

# Annotate endpoints
router.add_api_route("/aps/annotate", annotate_ap, methods=["POST"])
router.add_api_route(
    "/aps/annotate/{semiring_name}", annotate_ap_with_semiring, methods=["POST"])

# Remove annotation endpoints
router.add_api_route("/aps/annotate", remove_annotation_ap, methods=["DELETE"])

# Explain endpoints
router.add_api_route("/aps/explain", explain_ap, methods=["POST"])
router.add_api_route(
    "/aps/explain/{semiring_name}", explain_ap_with_semiring, methods=["POST"])

# Health check
router.add_api_route("/health", health_check, methods=["GET"])
