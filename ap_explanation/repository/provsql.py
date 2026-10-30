"""
How this service labels the rows of an annotated table for ProvSQL.

Each row carries a surrogate reference, a value rather than its physical
location (the CTID this replaced), so it survives UPDATE, VACUUM FULL and
ProvSQL's own data-modification rewrites.

The reference lives in a plain column with a DEFAULT (rather than a GENERATED
column) for two reasons: ProvSQL's ``provenance_guard`` trigger reads it as
``NEW.provsql_ref`` in a BEFORE INSERT trigger, where a generated column is not
yet computed, and a DEFAULT expression cannot reference other columns anyway —
which rules out deriving the reference from a primary key, and lets tables
without one work the same way.
"""

from re import compile

# Name of the column added to every annotated table to hold its stable row
# reference. It is also the mapping attribute handed to
# create_provenance_mapping, which is what makes maintained mappings possible:
# provenance_guard interpolates the attribute as ($1).%I, so it has to name a
# real column rather than an expression.
REFERENCE_COLUMN = "provsql_ref"

# Suffix of a table's mapping table, shared by all semirings: the reference
# identifies the row, not the provenance flavour.
MAPPING_TABLE_SUFFIX = "_provkey"

_REFERENCE_RE = compile(r"([a-zA-Z_][a-zA-Z0-9_]*)@k([0-9a-f]+)")


def reference_default(table_name: str) -> str:
    """
    SQL DEFAULT expression populating the reference column.

    A random suffix rather than a key-derived one: the expression must be
    self-contained, and the hex alphabet keeps the reference unambiguous
    inside the ``{…}`` / ``,`` / ``⊗`` / ``⊕`` syntax the semirings emit.
    """
    literal = table_name.replace("'", "''")
    return f"'{literal}@k' || translate(gen_random_uuid()::text, '-', '')"


def references_in(expression: str) -> list[tuple[str, str]]:
    """
    The ``(table, reference)`` pairs a provenance expression names.

    Args:
        expression: Supports both brace-wrapped format (formula semiring):
                      {{table@k<hex>}⊗{table@k<hex>}}
                    and comma-separated format (why-provenance semiring):
                      {"{table@k<hex>,table@k<hex>,...}"}
    """
    return [(table, f"{table}@k{key}") for table, key in _REFERENCE_RE.findall(expression)]
