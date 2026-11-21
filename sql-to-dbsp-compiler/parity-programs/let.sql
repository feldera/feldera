SET feldera_window_sharing_threshold = 1;
CREATE TABLE t(id INT NOT NULL PRIMARY KEY, ts TIMESTAMP);
CREATE VIEW v AS SELECT id FROM t WHERE ts = NOW();
CREATE VIEW w AS SELECT id FROM t WHERE NOW() = ts AND id > 1;
