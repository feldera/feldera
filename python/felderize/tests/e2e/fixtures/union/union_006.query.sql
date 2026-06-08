CREATE VIEW union_006 AS
SELECT col1, col2, col3, NULLIF('','') AS col4
FROM jsonTable
UNION ALL
SELECT col2, col2, null AS col3, col4
FROM jsonTable;
