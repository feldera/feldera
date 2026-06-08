CREATE VIEW string-functions_099 AS
select encode(decode(encode('白日依山尽，黄河入海流。欲穷千里目，更上一层楼。', 'UTF-16'), 'UTF-16'), 'UTF-8');
