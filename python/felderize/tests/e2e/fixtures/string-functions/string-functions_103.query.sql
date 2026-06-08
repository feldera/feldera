CREATE VIEW string-functions_103 AS
select encode(decode(encode('Sparkは世界で最も人気のあるビッグデータ処理フレームワークである。', 'UTF-16'), 'UTF-16'), 'UTF-8');
