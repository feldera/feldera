CREATE OR REPLACE TEMP VIEW bm76_returning_non_buyers AS
SELECT user_id FROM returners EXCEPT SELECT user_id FROM buyers;
