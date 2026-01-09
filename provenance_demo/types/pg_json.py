from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class PgJsonNode(BaseModel):
    id: str
    labels: List[str]
    properties: Optional[Dict[str, Any]] = None


class PgJsonEdge(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    labels: List[str]
    to: str
    properties: Optional[Dict[str, Any]] = None

    @property
    def from_id(self) -> str:
        """Backward compatibility property"""
        return self.from_


class PgJson(BaseModel):
    nodes: List[PgJsonNode]
    edges: List[PgJsonEdge]

    def get_node_by_id(self, node_id: str) -> Optional[PgJsonNode]:
        return next((n for n in self.nodes if n.id == node_id), None)

    def get_edges_from(self, node_id: str) -> List[PgJsonEdge]:
        return [e for e in self.edges if e.from_ == node_id]

    def get_edges_to(self, node_id: str) -> List[PgJsonEdge]:
        return [e for e in self.edges if e.to == node_id]

    def get_nodes_by_label(self, label: str) -> List[PgJsonNode]:
        return [n for n in self.nodes if label in n.labels]
