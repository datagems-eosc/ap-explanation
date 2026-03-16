from fastapi import APIRouter

from .annotations import annotate_ap, annotate_ap_with_semiring, remove_annotation_ap
from .computations import compute_provenance_ap, compute_provenance_ap_with_semiring

router = APIRouter(prefix="/aps/explanation/manual")

router.add_api_route("/annotations", annotate_ap, methods=["POST"])
router.add_api_route(
    "/annotations/{semiring_name}", annotate_ap_with_semiring, methods=["POST"])
router.add_api_route("/annotations", remove_annotation_ap, methods=["DELETE"])
router.add_api_route("/computations", compute_provenance_ap, methods=["POST"])
router.add_api_route("/computations/{semiring_name}",
                     compute_provenance_ap_with_semiring, methods=["POST"])
