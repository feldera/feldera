CREATE VIEW null-handling_011 AS
select a+60, case when b<>0 and c<>0 then 1 else 0 end from t1;
