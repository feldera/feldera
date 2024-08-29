-- Set all programs to be automatically compiled. Removes 'none'
-- as a valid status field in the database.
UPDATE program SET status = 'pending' WHERE status = 'none';
