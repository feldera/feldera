CREATE OR REPLACE TEMP VIEW val14_flat_tags AS
SELECT row_id, flatten(nested_tags) AS flat_tags
FROM collection_events;
