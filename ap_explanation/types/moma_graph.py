"""
The MoMa PG-JSON graph primitives, re-exported from ``moma-domain``.

The MoMa vocabulary (node types, edge labels, property schemas) is owned by
moma-management; this service consumes it rather than redefining it. Importing
through this module keeps the deep ``moma_management.domain.generated.*`` paths
in one place.

Two things to know when handling these models:

* ``Node.id``, ``Edge.from_`` and ``Edge.to`` are :class:`~uuid.UUID`, not
  ``str``. Compare them as ``str(...)`` when matching against ids that came in
  as text.
* ``Edge.labels`` holds :class:`EdgeLabel` members of a *closed* enum, and
  ``EdgeLabel`` is a plain ``Enum`` rather than a ``str`` enum — so
  ``"input" in edge.labels`` is always ``False``. Compare against the member
  (``EdgeLabel.input in edge.labels``).
"""

from moma_management.domain.generated.edges.edge_schema import Edge, EdgeLabel
from moma_management.domain.generated.nodes.node_schema import Node

__all__ = ["Edge", "EdgeLabel", "Node"]
