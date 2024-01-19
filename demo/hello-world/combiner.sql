CREATE TABLE messages (id INTEGER, message STRING);
CREATE TABLE records (id INTEGER, message STRING);

CREATE VIEW matches AS
SELECT messages.message || ' ' || records.message AS combined
FROM messages
JOIN records
    ON messages.id = records.id;
