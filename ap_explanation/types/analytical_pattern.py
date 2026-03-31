from typing import Self

from pydantic import model_validator

from .pg_json import PgJson, PgJsonNode


# NOTE: This is meant to be loaded from a lib at some point
class AnalyticalPattern(PgJson):

    _root: PgJsonNode

    @model_validator(mode="after")
    def check_root_node(self: Self) -> Self:
        ROOT_LABEL = "Analytical_Pattern"

        # Basic check
        ap_nodes = [n for n in self.nodes if ROOT_LABEL in n.labels]
        if not ap_nodes:
            raise ValueError(f"No '{ROOT_LABEL}' nodes found")

        if len(ap_nodes) > 1:
            root_ids = ", ".join(n.id for n in ap_nodes)
            raise ValueError(
                f"Multi-root AP detected (root nodes ids: {root_ids})"
            )

        root = ap_nodes[0]

        if not root.id:
            raise ValueError(f"The root '{ROOT_LABEL}' node has no id!")

        self._root = root

        # Check that the root node is truly a root (no edges lead to it)
        edges_to_root = [e for e in self.edges if e.to == root.id]
        if edges_to_root:
            edge_sources = ", ".join(
                f"({e.from_} -> {e.to})" for e in edges_to_root)
            raise ValueError(
                f"The root '{ROOT_LABEL}' node is not a root. "
                f"The following edges lead to it: {edge_sources}"
                "Did you leave the Task node in the AP graph?"
            )

        # Ensure the undirected graph is properly connected to the root
        # i.e : "Ensure all nodes are reachable from the root, no matter the direction"
        reachable = set(self._dfs_iter_undirected(self.root.id))
        all_ids = {n.id for n in self.nodes}

        if reachable != all_ids:
            if reachable - all_ids:
                # Reaching more nodes than existing ones -> An edge references a missing node
                extra = ", ".join(sorted(reachable - all_ids))
                raise ValueError(
                    f"Graph traversal returned unknown node IDs: {extra}. "
                    f"Edges may reference missing nodes."
                )

            if all_ids - reachable:
                # There are nodes not reachable from the root
                unreachable = ", ".join(sorted(all_ids - reachable))
                raise ValueError(
                    f"Graph is not fully connected. "
                    f"Unreachable nodes from root: {unreachable}"
                )
        return self

    @property
    def root(self) -> PgJsonNode:
        """Return the AP root node"""
        return self._root

    def normalize(self) -> Self:
        """
        Normalize the AP in place:
        - Sorts nodes by id
        - Sorts edges by from_, to, labels
        - Sorts labels alphabetically
        """
        for n in self.nodes:
            if getattr(n, "labels", None):
                n.labels = sorted(n.labels)
        self.nodes.sort(key=lambda n: n.id)

        for e in self.edges:
            if getattr(e, "labels", None):
                e.labels = sorted(e.labels)
        self.edges.sort(key=lambda e: (e.from_, e.to, tuple(e.labels)))
        return self
