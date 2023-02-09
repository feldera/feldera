CREATE TABLE messages (id INTEGER, message STRING);
CREATE TABLE records (id INTEGER, message STRING);

CREATE VIEW message_combiner AS
SELECT messages.message || ' ' || records.message
FROM messages
JOIN records
    ON messages.id = records.id;
