CREATE VIEW string-functions_005 AS
select position('bar' in 'foobarbar'), position(null, 'foobarbar'), position('aaads', null);
