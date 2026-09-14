-- All answers of student 80: no DISTINCT nor GROUP BY, rows are never merged
SELECT a.id, a.answer
FROM assessment a
WHERE a.student_id=80;
