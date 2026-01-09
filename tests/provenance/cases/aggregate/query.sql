-- All wrong answers to difficult questions by student 80, by topic
SELECT t.name, count(distinct a.question_id) AS nb_questions
FROM assessment a JOIN platform__topic t ON (a.topic=t.id) 
WHERE student_id=80 AND answer=-1 AND question_level>2 
GROUP BY t.name;