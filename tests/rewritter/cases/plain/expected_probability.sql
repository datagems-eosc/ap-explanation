-- Appended in place, like the semiring pass: ProvSQL gives a wrapped join a different token
SELECT a.id, a.answer, probability_evaluate(provenance())
FROM assessment a
WHERE a.student_id=80;
