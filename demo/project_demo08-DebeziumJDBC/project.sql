create table test_table(
    id bigint not null primary key,
    f1 boolean,
    f2 string,
    f3 tinyint,
    f4 decimal(5,2),
    f5 float64,
    f6 time,
    f7 timestamp,
    f8 date,
    f9 binary
);

create view test_view as select * from test_table;