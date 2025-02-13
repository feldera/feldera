CREATE FUNCTION SPLIT_INDEX(s VARCHAR, sep CHAR, idx INT) RETURNS VARCHAR
AS SPLIT(s, CAST(sep AS VARCHAR))[idx + 1];

CREATE VIEW Q22 AS
SELECT
    auction, bidder, price, channel,
    SPLIT_INDEX(url, '/', 3) as dir1,
    SPLIT_INDEX(url, '/', 4) as dir2,
    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;
