CREATE TYPE item AS (i INT, s VARCHAR);
CREATE TABLE t (v item);
CREATE VIEW v AS SELECT v NOT IN (ROW(4, 'cat')) AS absent FROM t;
