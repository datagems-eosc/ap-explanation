SELECT probability_evaluate(provenance())
FROM (
    SELECT a.id, a.answer
    FROM assessment a
    WHERE a.student_id=80
) AS x;
