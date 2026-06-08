CREATE VIEW string-functions_120 AS
select decode(scol, ecol) from values(X'E58A9DE5909BE69BB4E5B0BDE4B880E69DAFE98592', 'US-ASCII') as t(scol, ecol);
