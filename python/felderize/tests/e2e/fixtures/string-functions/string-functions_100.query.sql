CREATE VIEW string-functions_100 AS
select encode(decode(encode('南山經之首曰䧿山。其首曰招搖之山，臨於西海之上。', 'UTF-16'), 'UTF-16'), 'UTF-8');
