CREATE VIEW malformed_077_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_077_b AS
SELECT id, val FROM base WHERE val > 6;

CREATE VIEW malformed_077 AS
SELECT id, val
FROM malformed_077_a JOIN malformed_077_b ON malformed_077_a.id = malformed_077_b.id;
