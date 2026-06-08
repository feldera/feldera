CREATE OR REPLACE TEMP VIEW bm70_normalized_arrays AS
SELECT session_id, sort_array(array_distinct(tags)) AS normalized_tags FROM session_tags2;
