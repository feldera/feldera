-- rule: array_type_syntax
-- spark: ARRAY<T> type syntax in DDL (e.g., ARRAY<STRING>, ARRAY<INT>)
-- feldera: T ARRAY suffix syntax — e.g. VARCHAR ARRAY, INT ARRAY. Never use ARRAY<T> in Feldera DDL
CREATE OR REPLACE TEMP VIEW student_scores_view AS SELECT student_id, name, test_scores, exam_date FROM student_scores WHERE ARRAY_CONTAINS(test_scores, 95);
