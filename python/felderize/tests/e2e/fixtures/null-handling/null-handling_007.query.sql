CREATE VIEW null-handling_007 AS
select a+20, case b when c then 1 else 0 end from t1;
