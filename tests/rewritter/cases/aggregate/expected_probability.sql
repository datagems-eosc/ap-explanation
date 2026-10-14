-- Wrapped: the probability is evaluated on each group's token, in the same shape as the formula
SELECT x.name, x.nb_questions, probability_evaluate(provenance())
FROM (
    SELECT t.name, count(distinct a.question_id) AS nb_questions
    FROM assessment a JOIN platform__topic t ON (a.topic=t.id)
    WHERE student_id=80 AND answer=-1 AND question_level>2
    GROUP BY t.name
) AS x
WHERE present(provenance());
