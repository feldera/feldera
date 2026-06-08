CREATE VIEW string-functions_102 AS
select encode(decode(encode('το Spark είναι το πιο δημοφιλές πλαίσιο επεξεργασίας μεγάλων δεδομένων παγκοσμίως', 'UTF-16'), 'UTF-16'), 'UTF-8');
