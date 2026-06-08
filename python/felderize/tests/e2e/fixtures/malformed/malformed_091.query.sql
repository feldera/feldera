CREATE VIEW malformed_091_a AS
SELECT id, k, val FROM base WHERE k = 'k0;

CREATE VIEW malformed_091 AS
SELECT id, val FROM malformed_091_a;
