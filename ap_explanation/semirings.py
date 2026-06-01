
from ap_explanation.repository.mapping.ctid_mapping import CtidMapping
from ap_explanation.types.semiring import DbSemiring

semirings = [
    DbSemiring(
        name="formula",
        retrieval_function="sr_formula",
        mapping_table="formula_mapping",
        mappingStrategy=CtidMapping(),
    ),
    DbSemiring(
        name="why",
        retrieval_function="sr_why",
        mapping_table="why_mapping",
        mappingStrategy=CtidMapping(),
    ),
    DbSemiring(
        name="boolexpr",
        retrieval_function="sr_boolexpr",
        mapping_table="boolexpr_mapping",
        mappingStrategy=CtidMapping(),
    ),
    DbSemiring(
        name="how",
        retrieval_function="sr_how",
        mapping_table="how_mapping",
        mappingStrategy=CtidMapping(),
    ),
    DbSemiring(
        name="which",
        retrieval_function="sr_which",
        mapping_table="which_mapping",
        mappingStrategy=CtidMapping(),
    ),
]
