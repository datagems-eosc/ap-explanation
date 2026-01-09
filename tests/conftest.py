import pytest

from provenance_demo.services.provenance import ProvenanceService


@pytest.fixture(scope="session")
def unit_prov_svc():
    """
    Offline ProvenanceService for testing purposes.
    This one does not connect to a real database.
    """
    return ProvenanceService(None)  # type: ignore
