-- rule: monotonically_increasing_id
-- spark: monotonically_increasing_id() — generates a unique, monotonically increasing 64-bit integer per row using partition metadata; nondeterministic across runs
-- feldera: UNSUPPORTED — Feldera has no equivalent; no stable row identity in streaming mode. Mark unsupported and suggest a surrogate key from existing columns or a sequence.
CREATE OR REPLACE TEMP VIEW session_ranks_v2 AS
SELECT
  monotonically_increasing_id() AS row_seq,
  user_id,
  session_key,
  session_duration,
  is_active
FROM user_sessions
WHERE is_active = TRUE
ORDER BY user_id;
