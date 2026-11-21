CREATE TYPE item AS (f INTEGER, label VARCHAR);
CREATE TABLE items (s item);
CREATE VIEW selected AS SELECT s['f'] AS f FROM items;
