-- Resets the `program_error` field in the `pipeline` table such
-- that it can be used to hold the new type.

UPDATE pipeline SET program_error = '{}'
                WHERE program_status = 'pending';

UPDATE pipeline SET program_error = '{}'
                WHERE program_status = 'compiling_sql';

UPDATE pipeline SET program_error = '{ "sql_compilation": { "exit_code": 0, "messages": [] } }'
                WHERE program_status = 'sql_compiled';

UPDATE pipeline SET program_error = '{ "sql_compilation": { "exit_code": 0, "messages": [] } }'
                WHERE program_status = 'compiling_rust';

UPDATE pipeline SET program_error = '{ "sql_compilation": { "exit_code": 0, "messages": [] }, "rust_compilation": { "exit_code": 0, "stdout": "", "stderr": "" } }'
                WHERE program_status = 'success';

UPDATE pipeline SET program_error = CONCAT('{ "sql_compilation": { "exit_code": 1, "messages": ', program_error, ' } }')
                WHERE program_status = 'sql_error';

UPDATE pipeline SET program_error = CONCAT('{ "sql_compilation": { "exit_code": 0, "messages": [] }, "rust_compilation": { "exit_code": 1, "stdout": "", "stderr": ', TO_JSON(program_error), ' } }')
                WHERE program_status = 'rust_error';

UPDATE pipeline SET program_error = CONCAT('{ "system_error": ', TO_JSON(program_error), ' }')
                WHERE program_status = 'system_error' AND program_info IS NULL;

UPDATE pipeline SET program_error = CONCAT('{ "sql_compilation": { "exit_code": 0, "messages": [] }, "system_error": ', TO_JSON(program_error), ' }')
                WHERE program_status = 'system_error' AND program_info IS NOT NULL;

UPDATE pipeline SET program_error = '{}' WHERE program_error IS NULL;
ALTER TABLE pipeline ALTER COLUMN program_error SET NOT NULL;
