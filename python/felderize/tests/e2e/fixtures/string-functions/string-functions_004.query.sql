CREATE VIEW string-functions_004 AS
select length(uuid()), (uuid() <> uuid());
