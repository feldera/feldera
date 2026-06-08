CREATE VIEW cast_040 AS
select cast(ym as interval year to month) from values(-122S) as t(ym);
