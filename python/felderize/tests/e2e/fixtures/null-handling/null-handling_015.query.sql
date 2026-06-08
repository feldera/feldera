CREATE VIEW null-handling_015 AS
select count(*), count(b), sum(b), avg(b), min(b), max(b) from t1;
