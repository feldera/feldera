CREATE VIEW null-handling_013 AS
select a+80, case when b<>0 or c<>0 then 1 else 0 end from t1;
