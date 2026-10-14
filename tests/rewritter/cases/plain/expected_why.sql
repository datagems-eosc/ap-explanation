-- Wrapped: present(provenance()) keeps the rows of the SQL answer, not those of other possible worlds
SELECT x.*, sr_why(provenance(), 'why_mapping')
FROM (
    SELECT a.id, a.answer
    FROM assessment a
    WHERE a.student_id=80
) AS x
WHERE present(provenance());
