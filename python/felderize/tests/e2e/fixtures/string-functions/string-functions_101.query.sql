CREATE VIEW string-functions_101 AS
select encode(decode(encode('세계에서 가장 인기 있는 빅데이터 처리 프레임워크인 Spark', 'UTF-16'), 'UTF-16'), 'UTF-8');
