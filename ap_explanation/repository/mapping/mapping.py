from typing import List, Protocol


class ProvenanceMapping[T](Protocol):
    """
    Abstract base class for provenance mapping strategies.

    Different semirings use different ways to map database rows to provenance values.
    This abstract class defines the interface that all mapping strategies must implement.
    """

    #: Column on the source table holding the encoded reference. It is handed
    #: to ``create_provenance_mapping`` as the mapping attribute, which is what
    #: makes ProvSQL's maintained mappings possible: ``provenance_guard``
    #: interpolates the attribute as ``($1).%I``, so it must name a real column
    #: rather than an expression.
    reference_column: str

    #: Column on the source table whose value a decoded reference is matched
    #: against when resolving rows.
    lookup_column: str

    def encode(self, table_name: str) -> str:
        """
        Generate the SQL expression populating the reference column for a table.

        Args:
            table_name: The name of the database table

        Returns:
            SQL expression string that generates the provenance reference
        """
        ...

    def decode(self, value: str) -> T:
        """
        Decode a single provenance reference back to a table/row reference.

        Raises:
            ValueError: if the value is not in this strategy's format. Callers
                use this to detect a mapping table built by another strategy.
        """
        ...

    def decode_equation(self, values: str) -> List[T]:
        """
        Decode a provenance equation string into a list of decoded references.

        Args:
            values: A string containing multiple provenance entries in this
                    strategy's encoded format.
        Returns:
            A list of decoded reference dictionaries.
        """
        ...

    def lookup_value(self, decoded: T) -> str:
        """Value of :attr:`lookup_column` matching a decoded reference."""
        ...

    def reference(self, decoded: T) -> str:
        """Reference string echoed back in the explanation output."""
        ...
