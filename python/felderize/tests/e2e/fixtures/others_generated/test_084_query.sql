CREATE OR REPLACE TEMP VIEW val85_overlap_flag AS
SELECT row_id, arrays_overlap(preferred_tags, recent_tags) AS has_overlap
FROM collection_events;
