CREATE OR REPLACE TEMP VIEW val01_nums_plus_one AS
SELECT row_id, transform(nums, x -> x + 1) AS nums_plus_one
FROM collection_events;
