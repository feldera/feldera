create table t1 (
    d date,
    -- TODO: JIT doesn't support the `TIME` type yet.
    -- t time,
    ts timestamp
);

create view v1 as select * from t1;