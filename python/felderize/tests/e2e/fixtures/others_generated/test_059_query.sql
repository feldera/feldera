CREATE OR REPLACE TEMP VIEW bm94_sorted_collected_tags AS
SELECT session_id, sort_array(collect_set(tag)) AS sorted_tags FROM session_tag_rows GROUP BY session_id;
