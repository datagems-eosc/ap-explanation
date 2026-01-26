from typing import List, Protocol


class ProvenanceMapping[T](Protocol):
    """
    Abstract base class for provenance mapping strategies.

    Different semirings use different ways to map database rows to provenance values.
    This abstract class defines the interface that all mapping strategies must implement.
    """

    def encode(self, table_name: str) -> str:
        """
        Generate the SQL expression to create a mapping target for a given table.

        Args:
            table_name: The name of the database table

        Returns:
            SQL expression string that generates the provenance reference
        """
        ...

    def decode(self, value: str) -> T:
        """
        Check if this mapping format can be decoded back to table/row references.

        Returns:
            True if decode_row_reference() is supported, False otherwise
        """
        ...

    def decode_equation(self, values: str) -> List[T]:
        """
        Decode a provenance equation string into a list of RowCtid dictionaries.
        Args:
            values: A string containing multiple provenance entries in the format
                    '{table_name@p<page>r<row>}'.
        Returns:
            A list of RowCtid dictionaries.
        """
        ...
