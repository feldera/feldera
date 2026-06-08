CREATE OR REPLACE TEMP VIEW val02_berry_tags AS
SELECT row_id, filter(tags, x -> x RLIKE '.*berry') AS berry_tags
FROM collection_events;
