
from ap_explanation.repository.mapping.ctid_mapping import CtidMapping
from ap_explanation.types.semiring import DbSemiring

semirings = [
    DbSemiring(
        name="formula",
        retrieval_function="formula",
        aggregate_function="aggregation_formula",
        mapping_table="formula_mapping",
        mappingStrategy=CtidMapping(),
    ),
    DbSemiring(
        name="why",
        retrieval_function="whyprov_now",
        # aggregate_function="aggregation_formula",
        mapping_table="why_mapping",
        mappingStrategy=CtidMapping(),
    ),
]
