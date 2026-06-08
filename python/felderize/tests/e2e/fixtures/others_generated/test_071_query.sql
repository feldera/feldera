CREATE OR REPLACE TEMP VIEW val25_summary_json AS
SELECT
  row_id,
  to_json(named_struct('row_id', row_id, 'tag_count', size(tags))) AS summary_json
FROM collection_events;
