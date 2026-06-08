CREATE VIEW cast_044 AS
select cast(dt as interval hour to second) from values(100Y) as t(dt);
