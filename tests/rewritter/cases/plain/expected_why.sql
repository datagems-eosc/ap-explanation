-- Appended in place: each result row is a single tuple
SELECT a.id, a.answer, sr_why(provenance(), 'why_mapping')
FROM assessment a
WHERE a.student_id=80;
