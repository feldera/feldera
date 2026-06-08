CREATE VIEW null-handling_020 AS
select a+140 from t1 where not (b<10 AND c=1);
