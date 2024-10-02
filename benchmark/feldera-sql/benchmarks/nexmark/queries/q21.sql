CREATE FUNCTION re_extract(s VARCHAR, p VARCHAR, grp INTEGER) RETURNS VARCHAR;

CREATE VIEW candidates AS
SELECT
    auction,
    bidder,
    price,
    channel,
    url,
    lower(channel) as lower_channel
FROM
    bid;

CREATE VIEW q21 AS
SELECT
    auction, bidder, price, channel,
    CASE
        WHEN lower_channel = 'apple' THEN '0'
        WHEN lower_channel = 'google' THEN '1'
        WHEN lower_channel = 'facebook' THEN '2'
        WHEN lower_channel = 'baidu' THEN '3'
        ELSE re_extract(url, CAST('(&|^)channel_id=([^&]*)' as VARCHAR), 2)
        END
    AS channel_id
FROM candidates
    where lower_channel in ('apple', 'google', 'facebook', 'baidu')
          or re_extract(url, CAST('(&|^)channel_id=([^&]*)' as VARCHAR), 2) is not null;