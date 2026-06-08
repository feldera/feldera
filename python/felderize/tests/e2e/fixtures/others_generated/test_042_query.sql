CREATE OR REPLACE TEMP VIEW bm58_unique_pairs AS
SELECT source, COUNT(DISTINCT named_struct('l', left_id, 'r', right_id)) AS unique_pair_count FROM pair_events GROUP BY source;
