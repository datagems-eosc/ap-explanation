from abc import abstractmethod
from collections.abc import Sequence

from moma_management.domain.generated.nodes.dataset.data_schema import Data
from pydantic import BaseModel


class DataSource(BaseModel):
    """Base class for data sources."""

    #: The nodes holding the tables' data, one per table
    data_nodes: Sequence[Data]

    @property
    @abstractmethod
    def db_name(self) -> str: ...

    @property
    @abstractmethod
    def schema_name(self) -> str: ...

    @abstractmethod
    def table_name(self, node: Data) -> str:
        """The name of the table holding the data of *node*."""
        ...

    @property
    def table_names(self) -> list[str]:
        return [self.table_name(node) for node in self.data_nodes]

    @property
    def probability_columns(self) -> dict[str, str | None]:
        """
        Map each table name to the column holding its tuples' probabilities, as
        declared by the data node's ``probabilityColumn`` property, or ``None``
        when the node declares none (every tuple of that table is certain).
        ``probabilityColumn`` is ap-explanation's own: no moma schema declares it.
        """
        return {
            self.table_name(node): (node.model_extra or {}).get("probabilityColumn")
            for node in self.data_nodes
        }
