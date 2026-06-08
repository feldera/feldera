CREATE OR REPLACE TEMP VIEW val186_array_union_distinct_tags AS
SELECT row_id, array_union(tags, tags2) AS merged_tags
FROM collection_events;
