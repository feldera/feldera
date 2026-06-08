CREATE VIEW malformed_025_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_025_b AS
SELECT id, val FROM malformed_025_a WHERE val > 4;

CREATE VIEW malformed_025 AS
SELECT id, val FROM malformed_025_b;
