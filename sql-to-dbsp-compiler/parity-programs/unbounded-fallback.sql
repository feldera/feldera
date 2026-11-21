CREATE TABLE warehouse(id INT NOT NULL PRIMARY KEY, parentId INT);
CREATE TABLE t(i INT) WITH ('append_only' = 'true');
CREATE VIEW v AS SELECT i FROM t;
