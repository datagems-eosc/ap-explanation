from typing import ClassVar, Self
from uuid import UUID

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.generated.edges.edge_schema import Edge, EdgeLabel
from moma_management.domain.generated.nodes.node_schema import Node
from moma_management.domain.validation import StructureStep, ValidationStep
from pydantic import Field, model_validator

from ap_explanation.domains.provenance.data_sources.csv_set_ds import CsvSetDataSource
from ap_explanation.domains.provenance.data_sources.relational_db_ds import (
    RelationalDbDataSource,
)

from .data_sources import DataSource


class ProvenanceAnalyticalPattern(AnalyticalPattern):
    """
    A MoMa Analytical Pattern with a provenance operator over one data source.

    Two things to know when handling moma's models: ``Node.id``, ``Edge.from_``
    and ``Edge.to`` are :class:`~uuid.UUID`, not ``str``; and ``EdgeLabel`` is a
    plain ``Enum``, so ``"input" in edge.labels`` is always ``False``: compare
    against the member (``EdgeLabel.input in edge.labels``).
    """

    PROVENANCE_OP: ClassVar[str] = "Provenance_SQL_Operator"
    # Not moma's full chain: it accepts the fixtures, but what ap-executor
    # sends has not been tried against it.
    validation_chain: ClassVar[ValidationStep] = StructureStep()
    data_source: DataSource | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def check_prov_structure(self: Self) -> Self:
        """
        Ensure this AP is a compatible Provenance AP, by checking for the presence of a Provenance node directly connected to the root.
        """
        prov_nodes = self.get_nodes_by_label(self.PROVENANCE_OP)
        if not prov_nodes:
            raise ValueError(
                f"No '{self.PROVENANCE_OP}' node found in the AP, "
                "which is required for a Provenance AP!"
            )

        # TODO: This should be allowed at some point
        if len(prov_nodes) > 1:
            prov_ids = ", ".join(str(n.id) for n in prov_nodes)
            raise ValueError(
                f"Multiple '{self.PROVENANCE_OP}' nodes detected (ids: {prov_ids}), "
                "which is not yet allowed for a Provenance AP!"
            )
        prov_node = prov_nodes[0]

        # Check the edge "input" from Data nodes to the provenance operator
        input_edges = [
            e for e in self.get_edges_to(prov_node.id)
            if EdgeLabel.input in e.labels
        ]
        if not input_edges:
            raise ValueError(
                f"The '{self.PROVENANCE_OP}' node (id: {prov_node.id}) has no incoming edges, "
                "but it should have at least one edge labeled 'input' from a Data node!"
            )

        # Collect the source nodes of these "input" edges and check they are in the allowed Data nodes
        found: list[DataSource] = []
        for edge in input_edges:
            node = self.get_node_by_id(edge.from_)
            if node is None:
                raise ValueError(
                    f"The '{self.PROVENANCE_OP}' node (id: {prov_node.id}) has an 'input' edge "
                    f"to a node (id: {edge.to}) that does not exist in the graph!"
                )
            ds: DataSource
            match node.labels:
                case labels if "RelationalDatabase" in labels:
                    table_nodes = self._contained_nodes(node)
                    ds = RelationalDbDataSource(
                        base_node=node, table_nodes=table_nodes)
                case labels if "CsvSet" in labels:
                    csv_nodes = self._contained_nodes(node)
                    ds = CsvSetDataSource(base_node=node, csv_nodes=csv_nodes)
                case _:
                    # A provenance operator composed into a larger AP legitimately
                    # has non-Data inputs: ap-executor carries an upstream
                    # operator's output on an `input` edge into the consuming
                    # operator's node, because that is where it looks for
                    # `properties.mapping` (see resolve_operator_input_values).
                    # Those edges are wiring, not data sources - skip them. The
                    # data source is still mandatory; the check just moved from
                    # "every input edge is a data source" to "at least one is",
                    # asserted below.
                    continue
            found.append(ds)

        if not found:
            raise ValueError(
                f"The '{self.PROVENANCE_OP}' node (id: {prov_node.id}) has no 'input' edge "
                "from a Data node (RelationalDatabase or CsvSet)!"
            )

        if len(found) > 1:
            raise ValueError(
                f"The '{self.PROVENANCE_OP}' node (id: {prov_node.id}) has multiple 'input' edges, "
                "but only a single data source per AP is supported!"
            )

        self.data_source = found[0]
        return self

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

    def get_node_by_id(self, node_id: UUID) -> Node | None:
        return next((n for n in self.nodes if n.id == node_id), None)

    def get_edges_from(self, node_id: UUID) -> list[Edge]:
        return [e for e in (self.edges or []) if e.from_ == node_id]

    def get_edges_to(self, node_id: UUID) -> list[Edge]:
        return [e for e in (self.edges or []) if e.to == node_id]

    def get_nodes_by_label(self, label: str) -> list[Node]:
        return [n for n in self.nodes if label in (n.labels or [])]

    def _contained_nodes(self, node: Node) -> list[Node]:
        """
        The leaf data items a container node holds.

        MoMa models containment with a single ``containedIn`` edge for every
        Data sub-type pair (``RelationalDatabase → Table``, ``CsvSet → CSV``),
        so both data sources resolve their children the same way.
        """
        return [
            child
            for e in self.get_edges_from(node.id)
            if EdgeLabel.contained_in in e.labels
            for child in [self.get_node_by_id(e.to)]
            if child is not None
        ]

    @property
    def sql_operator(self) -> Node:
        """Return the Provenance_SQL_Operator node (guaranteed by check_prov_structure)."""
        return self.get_nodes_by_label(self.PROVENANCE_OP)[0]
