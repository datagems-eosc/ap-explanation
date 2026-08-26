from ap_explanation.repository.mapping.key_mapping import KeyMapping
from ap_explanation.types.semiring import DbSemiring

semirings = [
    DbSemiring(
        name="formula",
        retrieval_function="sr_formula",
        mapping_table="formula_mapping",
        mappingStrategy=KeyMapping(),
    ),
    DbSemiring(
        name="why",
        retrieval_function="sr_why",
        mapping_table="why_mapping",
        mappingStrategy=KeyMapping(),
    ),
    DbSemiring(
        name="boolexpr",
        retrieval_function="sr_boolexpr",
        mapping_table="boolexpr_mapping",
        mappingStrategy=KeyMapping(),
    ),
    DbSemiring(
        name="how",
        retrieval_function="sr_how",
        mapping_table="how_mapping",
        mappingStrategy=KeyMapping(),
    ),
    DbSemiring(
        name="which",
        retrieval_function="sr_which",
        mapping_table="which_mapping",
        mappingStrategy=KeyMapping(),
    ),
]
