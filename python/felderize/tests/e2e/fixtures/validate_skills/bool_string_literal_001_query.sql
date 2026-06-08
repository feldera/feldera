-- rule: bool_string_literal
-- spark: col = 'true' / col = 'false' — comparison of a STRING column to a quoted boolean string literal; Spark treats 'true'/'false' as string values when the column type is STRING
-- feldera: Keep the string literal as-is: col = 'true' / col = 'false' — only convert unquoted Python-style True/False to TRUE/FALSE boolean literals; if the value is already quoted, it is a string comparison
CREATE OR REPLACE TEMP VIEW active_users_v1 AS SELECT user_id, name FROM user_status_1 WHERE status = 'true';
