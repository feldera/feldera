CREATE VIEW q1 AS
SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    date_time,
    extra
FROM bid;