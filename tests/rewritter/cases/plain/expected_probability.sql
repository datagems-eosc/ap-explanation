-- Wrapped like the semiring pass: present(provenance()) keeps the rows of the SQL answer
SELECT x.*, probability_evaluate(provenance())
FROM (
    SELECT a.id, a.answer
    FROM assessment a
    WHERE a.student_id=80
) AS x
WHERE present(provenance());
