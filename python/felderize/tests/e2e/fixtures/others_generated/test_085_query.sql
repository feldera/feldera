CREATE OR REPLACE TEMP VIEW val88_reversed_tags AS
SELECT row_id, reverse(tags) AS reversed_tags
FROM collection_events;
