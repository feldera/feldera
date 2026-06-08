-- rule: arrays_overlap
-- spark: arrays_overlap(a, b) — true if arrays share any element
-- feldera: ARRAYS_OVERLAP(a, b)
CREATE OR REPLACE TEMP VIEW subject_overlap_v2 AS SELECT student_id, enrolled_subjects, arrays_overlap(enrolled_subjects, array(101, 102, 103)) AS takes_core_subjects FROM student_subjects;
