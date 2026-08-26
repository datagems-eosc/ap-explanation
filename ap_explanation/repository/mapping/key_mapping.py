from re import compile
from typing import List, TypedDict

from .mapping import ProvenanceMapping

# Name of the column added to every annotated table to hold its stable row
# reference. Shared by all semirings: the reference identifies the row, not
# the provenance flavour.
REFERENCE_COLUMN = "provsql_ref"

_REFERENCE_RE = compile(r"([a-zA-Z_][a-zA-Z0-9_]*)@k([0-9a-f]+)")


class RowReference(TypedDict):
    # Table name
    table: str
    # Full encoded reference, e.g. 'assessment@k0f3c...'
    reference: str


class KeyMapping(ProvenanceMapping[RowReference]):
    """
    Maps each row to a surrogate reference stored in a dedicated column.

    The reference is a value the row carries rather than its physical location
    (the CTID this replaced), so it survives UPDATE, VACUUM FULL and ProvSQL's
    own data-modification rewrites.

    The reference lives in a plain column with a DEFAULT (rather than a
    GENERATED column) for two reasons: ProvSQL's ``provenance_guard`` trigger
    reads it as ``NEW.provsql_ref`` in a BEFORE INSERT trigger, where a
    generated column is not yet computed, and a DEFAULT expression cannot
    reference other columns anyway — which rules out deriving the reference
    from a primary key, and lets tables without one work the same way.
    """

    # The mapping attribute handed to create_provenance_mapping is this
    # column's name, which is what makes maintained mappings possible:
    # provenance_guard interpolates the attribute as ($1).%I, so it has to
    # name a real column rather than an expression.
    reference_column = REFERENCE_COLUMN
    lookup_column = REFERENCE_COLUMN

    def encode(self, table_name: str) -> str:
        """
        SQL DEFAULT expression populating the reference column.

        A random suffix rather than a key-derived one: the expression must be
        self-contained, and the hex alphabet keeps the reference unambiguous
        inside the ``{…}`` / ``,`` / ``⊗`` / ``⊕`` syntax the semirings emit.
        """
        literal = table_name.replace("'", "''")
        return f"'{literal}@k' || translate(gen_random_uuid()::text, '-', '')"

    def decode(self, value: str) -> RowReference:
        match = _REFERENCE_RE.fullmatch(value)

        if not match:
            raise ValueError(f"Invalid provenance format: {value}")

        return {
            "table": match.group(1),
            "reference": value,
        }

    def decode_equation(self, values: str) -> List[RowReference]:
        """
        Decode a provenance equation string into a list of RowReference dicts.

        Args:
            values: A string containing multiple provenance entries.
                    Supports both brace-wrapped format (formula semiring):
                      {{table@k<hex>}⊗{table@k<hex>}}
                    and comma-separated format (why-provenance semiring):
                      {"{table@k<hex>,table@k<hex>,...}"}
        Returns:
            A list of RowReference dictionaries.
        """
        return [
            {
                "table": table,
                "reference": f"{table}@k{key}",
            }
            for table, key in _REFERENCE_RE.findall(values)
        ]

    def lookup_value(self, decoded: RowReference) -> str:
        return decoded["reference"]

    def reference(self, decoded: RowReference) -> str:
        return decoded["reference"]
