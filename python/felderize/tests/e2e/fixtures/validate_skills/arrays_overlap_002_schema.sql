-- rule: arrays_overlap
-- spark: arrays_overlap(a, b) — true if arrays share any element
-- feldera: ARRAYS_OVERLAP(a, b)
CREATE TABLE student_subjects (student_id INT, enrolled_subjects ARRAY<INT>);
