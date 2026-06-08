CREATE OR REPLACE TEMP VIEW bm77_multi_channel_users AS
SELECT user_id FROM app_users INTERSECT SELECT user_id FROM store_users;
