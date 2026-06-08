CREATE VIEW malformed_022_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_022_b AS
SELECT id, val FROM malformed_022_a WHERE val > 1;

CREATE VIEW malformed_022 AS
SELECT id, val FROM malformed_022_b;
