CREATE OR REPLACE TEMP VIEW val105_preferred_int AS
SELECT row_id, grp, if(a_int > b_int, a_int, b_int) AS preferred_int
FROM scalar_function_rows
WHERE if(a_int > b_int, a_int, b_int) >= (SELECT AVG(CAST(if(a_int > b_int, a_int, b_int) AS DOUBLE)) FROM scalar_function_rows);
