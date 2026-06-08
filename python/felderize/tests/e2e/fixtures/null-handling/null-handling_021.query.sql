CREATE VIEW null-handling_021 AS
select a+150 from t1 where not (c=1 AND b<10);
