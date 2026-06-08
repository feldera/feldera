CREATE VIEW null-handling_008 AS
select a+30, case c when b then 1 else 0 end from t1;
