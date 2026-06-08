-- rule: lateral_view_explode
-- spark: LATERAL VIEW explode(arr) t AS item — unnest array column into rows
-- feldera: CROSS JOIN LATERAL UNNEST(arr) AS t(item)  or  FROM t, UNNEST(arr) AS u(item)
CREATE OR REPLACE TEMP VIEW score_breakdown_v2 AS SELECT student_id, student_name, score FROM student_scores_002 LATERAL VIEW explode(exam_scores) s AS score;
