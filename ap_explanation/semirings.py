
from ap_explanation.repository.mapping.ctid_mapping import CtidMapping
from ap_explanation.types.semiring import DbSemiring

semirings = [
    DbSemiring(
        name="formula",
        retrieval_function="sr_formula",
        # aggregate_function="aggregation_formula",
        mapping_table="formula_mapping",
        mappingStrategy=CtidMapping(),
    ),
    DbSemiring(
        name="why",
        retrieval_function="whyPROV_now",
        # aggregate_function="aggregation_formula",
        mapping_table="why_mapping",
        mappingStrategy=CtidMapping(),
    ),
    DbSemiring(
        name="boolean",
        retrieval_function="bool_formula",
        # aggregate_function="aggregation_formula",
        mapping_table="bool_mapping",
        mappingStrategy=CtidMapping(),
    ),
]
