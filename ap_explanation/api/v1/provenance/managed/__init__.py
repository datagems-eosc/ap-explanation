from fastapi import APIRouter

from .managed import managed_provenance_ap, managed_provenance_ap_with_semiring
from .task_status import get_managed_task_status

router = APIRouter(prefix="/aps/explanation", tags=["managed"])

router.add_api_route("", managed_provenance_ap, methods=["POST"])
router.add_api_route("/{semiring_name}",
                     managed_provenance_ap_with_semiring, methods=["POST"])
router.add_api_route("/{task_id}", get_managed_task_status, methods=["GET"])
