CREATE VIEW null-handling_010 AS
select a+50, case when not b<>0 then 1 else 0 end from t1;
