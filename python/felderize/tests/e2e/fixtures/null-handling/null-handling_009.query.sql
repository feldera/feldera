CREATE VIEW null-handling_009 AS
select a+40, case when b<>0 then 1 else 0 end from t1;
