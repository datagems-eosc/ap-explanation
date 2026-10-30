from ap_explanation.domains.semiring import DbSemiring

semirings = [
    DbSemiring(
        name="formula",
        retrieval_function="sr_formula",
        supports_aggregates=True,
        rows_function="sr_formula_rows",
    ),
    DbSemiring(name="why", retrieval_function="sr_why"),
    DbSemiring(name="boolexpr", retrieval_function="sr_boolexpr"),
    DbSemiring(name="how", retrieval_function="sr_how"),
    DbSemiring(name="which", retrieval_function="sr_which"),
]
