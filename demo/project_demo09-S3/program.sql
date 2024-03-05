create table test_table(
    id integer not null primary key
);

create view test_view as select * from test_table;
