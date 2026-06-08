CREATE VIEW null-handling_014 AS
select a+90, case when not (b<>0 or c<>0) then 1 else 0 end from t1;
