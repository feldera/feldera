CREATE VIEW window_020 AS
select * from (select *, dense_rank() over (partition by p order by o) as rnk from t1) where rnk = 1;
