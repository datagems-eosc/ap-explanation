from typing import ClassVar, List, Optional, Self, Union
from uuid import UUID

from moma_management.domain.pg_json_graph import MomaEntity
from moma_management.domain.validation import StructureStep, ValidationStep
from pydantic import model_validator

from .moma_graph import Edge, Node

NodeId = Union[str, UUID]


class AnalyticalPattern(MomaEntity):
    """
    A MoMa Analytical Pattern graph.

    The graph models, the structural rules and ``normalize``/``difference`` all
    come from ``moma-domain`` — see :mod:`ap_explanation.types.moma_graph`.

    Only :class:`StructureStep` is run, not moma's full
    ``SchemaStep() & StructureStep() & MappingStep()`` chain. The APs this
    service handles carry ``Provenance_SQL_Operator`` and
    ``Provenance_Annotate_Dataset_Operator`` nodes, which have no schema in
    moma-management (it has no notion of provenance at all), so ``SchemaStep``
    would reject every AP we are asked to explain. Drop this override and
    inherit ``moma_management.domain.analytical_pattern.AnalyticalPattern``
    directly once those node types and their edge constraints are upstreamed.
    """

    _root_label: ClassVar[str] = "Analytical_Pattern"
    validation_chain: ClassVar[ValidationStep] = StructureStep()

    @model_validator(mode="after")
    def validate(self: Self) -> Self:
        errors = self.__class__.validation_chain.handle(self)
        if errors:
            raise ValueError(
                f"AnalyticalPattern validation failed with errors: {errors}")
        return self

    @property
    def root(self) -> Node:
        """Return the single ``Analytical_Pattern`` root node."""
        return next(n for n in self.nodes if self.__class__._root_label in n.labels)

    def to_wire(self) -> dict:
        """
        Dump the AP back to its JSON wire form.

        Always use this rather than a bare ``model_dump`` when the result will
        be validated again — over a Celery boundary, say. ``Edge.from_`` is
        populated from the ``from`` alias and moma's model does not set
        ``populate_by_name``, so a dump without ``by_alias`` produces a
        ``from_`` key that no longer parses.
        """
        return self.model_dump(mode="json", by_alias=True)

    # Lookup helpers. Node ids are UUIDs in the MoMa schema, so each one matches
    # on the string form and callers may pass either a UUID or its text form.

    def get_node_by_id(self, node_id: NodeId) -> Optional[Node]:
        target = str(node_id)
        return next((n for n in self.nodes if str(n.id) == target), None)

    def get_edges_from(self, node_id: NodeId) -> List[Edge]:
        target = str(node_id)
        return [e for e in (self.edges or []) if str(e.from_) == target]

    def get_edges_to(self, node_id: NodeId) -> List[Edge]:
        target = str(node_id)
        return [e for e in (self.edges or []) if str(e.to) == target]

    def get_nodes_by_label(self, label: str) -> List[Node]:
        return [n for n in self.nodes if label in (n.labels or [])]
