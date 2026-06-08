CREATE VIEW malformed_087_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_087 AS
SELECT id, val FROM malformed_087_a WHERE
