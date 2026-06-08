CREATE VIEW malformed_092_a AS
SELECT id, k, val FROM base WHERE k = 'k1;

CREATE VIEW malformed_092 AS
SELECT id, val FROM malformed_092_a;
