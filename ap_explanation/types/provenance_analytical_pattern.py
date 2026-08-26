from typing import ClassVar, List, Optional, Self

from pydantic import Field, model_validator

from ap_explanation.types.data_sources.csv_set_ds import CsvSetDataSource
from ap_explanation.types.data_sources.relational_db_ds import RelationalDbDataSource

from .analytical_pattern import AnalyticalPattern
from .data_sources import DataSource
from .moma_graph import EdgeLabel, Node


class ProvenanceAnalyticalPattern(AnalyticalPattern):

    PROVENANCE_OP: ClassVar[str] = "Provenance_SQL_Operator"
    data_source: Optional[DataSource] = Field(default=None, exclude=True)

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
        found: List[DataSource] = []
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
                    raise ValueError(
                        f"The '{self.PROVENANCE_OP}' node (id: {prov_node.id}) has an 'input' edge to a node (id: {node.id}) "
                        "which is not a valid Data node (RelationalDatabase or CsvSet)!"
                    )
            found.append(ds)

        if len(found) > 1:
            raise ValueError(
                f"The '{self.PROVENANCE_OP}' node (id: {prov_node.id}) has multiple 'input' edges, "
                "but only a single data source per AP is supported!"
            )

        self.data_source = found[0]
        return self

    def _contained_nodes(self, node: Node) -> List[Node]:
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
