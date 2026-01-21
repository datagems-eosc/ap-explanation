-- enabling plugin, setting search path
create extension provsql cascade;
set search_path to provsql_test,provsql,public,mathe;

-- add provenance to tables
select add_provenance('assessment');
select add_provenance('platform__sna__questions');
select add_provenance('platform__topic');
select add_provenance('platform__subtopic');

-- create provenance mappings for formula
select create_provenance_mapping('assessment_id', 'assessment', '''a''||id');
select create_provenance_mapping('question_id', 'platform__sna__questions', '''q''||id');
select create_provenance_mapping('topic_id', 'platform__topic', '''t''||id');
select create_provenance_mapping('subtopic_id', 'platform__subtopic', '''s''||id');
create table formula_mapping as
	select * from assessment_id union
	select * from question_id union
	select * from topic_id union
	select * from subtopic_id;
-- create provenance mappings for counting
select create_provenance_mapping('assessment_c', 'assessment', '1');
select create_provenance_mapping('question_c', 'platform__sna__questions', '1');
select create_provenance_mapping('topic_c', 'platform__topic', '1');
select create_provenance_mapping('subtopic_c', 'platform__subtopic', '1');
create table counting_mapping as
	select * from assessment_c union
	select * from question_c union
	select * from topic_c union
	select * from subtopic_c;
-- create provenance mappings for why
select create_provenance_mapping('assessment_why', 'assessment', '''a''||id');
select create_provenance_mapping('question_why', 'platform__sna__questions', '''q''||id');
select create_provenance_mapping('topic_why', 'platform__topic', '''t''||id');
select create_provenance_mapping('subtopic_why', 'platform__subtopic', '''s''||id');
create table why_mapping as
	select * from assessment_why union
	select * from question_why union
	select * from topic_why union
	select * from subtopic_why;
ALTER TABLE why_mapping ALTER COLUMN value TYPE varchar;
UPDATE why_mapping set value = '{"{' || value || '}"}';  
ALTER TABLE why_mapping ADD PRIMARY KEY (provenance);