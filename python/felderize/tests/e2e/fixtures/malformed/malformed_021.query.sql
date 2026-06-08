CREATE VIEW malformed_021_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_021_b AS
SELECT id, val FROM malformed_021_a WHERE val > 0;

CREATE VIEW malformed_021 AS
SELECT id, val FROM malformed_021_b;
