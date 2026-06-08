CREATE VIEW malformed_024_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_024_b AS
SELECT id, val FROM malformed_024_a WHERE val > 3;

CREATE VIEW malformed_024 AS
SELECT id, val FROM malformed_024_b;
