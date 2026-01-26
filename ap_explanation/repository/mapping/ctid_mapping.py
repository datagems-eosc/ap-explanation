from re import compile, search
from typing import List, Tuple, TypedDict

from .mapping import ProvenanceMapping


class RowCtid(TypedDict):
    # Table name
    table: str
    # Page number
    page: int
    # Row number
    row: int


class CtidMapping(ProvenanceMapping[RowCtid]):
    """
    Maps each row to its CTID in the format 'table_name@p{page}r{row}'.
    """

    def encode(self, table_name: str) -> str:
        return f"'{table_name}@p'||(ctid::text::point)[0]::int||'r'||(ctid::text::point)[1]::int"

    def decode(self, value: str) -> RowCtid:
        if '@' not in value:
            raise ValueError(f"Invalid provenance format: {value}")

        table_name, ctid_part = value.split('@', 1)
        match = search(r'p(\d+)r(\d+)', ctid_part)

        if not match:
            raise ValueError(f"Invalid ctid format in: {value}")

        page, row = match.groups()

        return {
            "table": table_name,
            "page": int(page),
            "row": int(row),
        }

    def decode_equation(self, values: str) -> List[RowCtid]:
        """
        Decode a provenance equation string into a list of RowCtid dictionaries.
        Args:
            values: A string containing multiple provenance entries in the format
                    '{table_name@p<page>r<row>}'.
        Returns:
            A list of RowCtid dictionaries.
        """
        reg = compile(r'\{([^{}@]+)@p(\d+)r(\d+)\}')
        return [
            {
                "table": table,
                "page": int(p),
                "row": int(r),
            }
            for table, p, r in reg.findall(values)
        ]
