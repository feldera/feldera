CREATE OR REPLACE TEMP VIEW bm508_gap_base64 AS
SELECT
  row_id,
  base64(CAST(payload AS BINARY)) AS payload_b64
FROM gap_hash_rows;
