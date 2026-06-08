CREATE VIEW cast_050 AS
select cast(interval '1 01:02:03.1' day to second as decimal(8, 1));
