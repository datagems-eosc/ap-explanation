SELECT DISTINCT t.name, whyPROV_now(provenance(),'why_mapping') 
FROM assessment a 
JOIN platform__sna__questions q ON(a.question_id=q.id) 
JOIN platform__topic t ON(t.id=q.topic) 
WHERE id_lect=78 AND answer=-1 AND question_level=4;
