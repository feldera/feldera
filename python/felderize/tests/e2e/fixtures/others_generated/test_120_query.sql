CREATE OR REPLACE TEMP VIEW bm507_gap_hash AS
SELECT
  row_id,
  md5(payload) AS md5_hex,
  sha2(payload, 256) AS sha256_hex
FROM gap_hash_rows;
