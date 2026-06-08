CREATE VIEW null-handling_022 AS
select b, c, equal_null(b, c), equal_null(c, b) from t1;
