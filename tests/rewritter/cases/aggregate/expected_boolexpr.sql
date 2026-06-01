SELECT x.name, sr_boolexpr(x.nb_questions, 'boolexpr_mapping') 
FROM (
    SELECT t.name, COUNT(DISTINCT a.question_id) AS nb_questions 
    FROM assessment AS a JOIN platform__topic AS t ON (a.topic = t.id) 
    WHERE student_id = 80 AND answer = -1 AND question_level > 2 GROUP BY t.name
) AS x;
