CREATE VIEW null-handling_012 AS
select a+70, case when not (b<>0 and c<>0) then 1 else 0 end from t1;
