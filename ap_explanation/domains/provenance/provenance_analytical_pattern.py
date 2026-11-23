from typing import Any, ClassVar, Self

from moma_management.domain.analytical_pattern import AnalyticalPattern
from moma_management.domain.generated.edges.edge_schema import EdgeLabel
from moma_management.domain.generated.nodes.dataset.csvSet_schema import CsvSet
from moma_management.domain.generated.nodes.dataset.data_schema import Data
from moma_management.domain.generated.nodes.dataset.relationalDatabase_schema import (
    RelationalDatabase,
)
from moma_management.domain.generated.nodes.dataset.table_schema import Table
from moma_management.domain.validation import StructureStep, ValidationStep
from pydantic import PrivateAttr, model_validator

from ap_explanation.domains.provenance.data_sources.csv_set_ds import CsvSetDataSource
from ap_explanation.domains.provenance.data_sources.relational_db_ds import (
    RelationalDbDataSource,
)

from .data_sources import DataSource


class ProvenanceAnalyticalPattern(AnalyticalPattern):
    """
    A MoMa Analytical Pattern with a provenance operator over one data source.
    """

    PROVENANCE_OP: ClassVar[str] = "Provenance_SQL_Operator"
    # Not moma's full chain: it accepts the fixtures, but what ap-executor
    # sends has not been tried against it.
    validation_chain: ClassVar[ValidationStep] = StructureStep()
    # Set by check_prov_structure, which refuses an AP without them
    _data_source: DataSource = PrivateAttr()
    _query: str = PrivateAttr()

    @model_validator(mode="after")
    def check_prov_structure(self: Self) -> Self:
        """
        Ensure this AP is a compatible Provenance AP, by checking for the presence of a Provenance node directly connected to the root.
        """
        prov_nodes = [n for n in self.nodes if self.PROVENANCE_OP in n.labels]
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

        # Read raw: typed as moma's Operator, a node without a name would be refused
        query = (prov_node.properties or {}).get("query")
        if not isinstance(query, str) or not query.strip():
            raise ValueError(
                f"The '{self.PROVENANCE_OP}' node (id: {prov_node.id}) has no 'query' property!")
        self._query = query

        # The data source feeds the provenance operator through an "input" edge.
        # Its other inputs are not Data: composed into a larger AP, ap-executor
        # carries an upstream operator's output on an `input` edge into the
        # consuming operator's node, because that is where it looks for
        # `properties.mapping` (see resolve_operator_input_values). Those edges
        # are wiring, not data sources.
        found: list[DataSource] = []
        for source in self.neighbors(prov_node, EdgeLabel.input, Data, incoming=True):
            match source:
                case RelationalDatabase():
                    found.append(RelationalDbDataSource(
                        base_node=source, data_nodes=self._contained(source, Table)))
                case CsvSet():
                    found.append(CsvSetDataSource(
                        base_node=source, data_nodes=self._contained(source, Data)))

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

        self._data_source = found[0]
        return self

    @property
    def data_source(self) -> DataSource:
        """The data source the provenance operator reads."""
        return self._data_source

    @property
    def query(self) -> str:
        """The SQL query of the provenance operator."""
        return self._query

    def to_wire(self) -> dict[str, Any]:
        """
        Dump the AP back to its JSON wire form.

        Always use this rather than a bare ``model_dump`` when the result will
        be validated again — over a Celery boundary, say. ``Edge.from_`` is
        populated from the ``from`` alias and moma's model does not set
        ``populate_by_name``, so a dump without ``by_alias`` produces a
        ``from_`` key that no longer parses.
        """
        return self.model_dump(mode="json", by_alias=True)

    def _contained[N: Data](self, container: Data, kind: type[N]) -> list[N]:
        """
        The *kind* items of *container*. ``containedIn`` runs from the item to its
        container, as moma has it (``Table -> RelationalDatabase``).
        """
        # shortcut: APs written before 2026-11 run containedIn from the container to
        # its items (the demos' APs); still read until they are rewritten
        items = (self.neighbors(container, EdgeLabel.contained_in, kind, incoming=True)
                 + self.neighbors(container, EdgeLabel.contained_in, kind))
        return list({item.id: item for item in items}.values())
