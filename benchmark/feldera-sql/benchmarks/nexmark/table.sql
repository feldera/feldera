CREATE TABLE person (
    id BIGINT,
    name VARCHAR,
    emailAddress VARCHAR,
    creditCard VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP(3) NOT NULL {lateness},
    extra  VARCHAR
) WITH ('connectors' = '{person}');
CREATE TABLE auction (
    id  BIGINT,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    date_time  TIMESTAMP(3) NOT NULL {lateness},
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    extra  VARCHAR
) WITH ('connectors' = '{auction}');
CREATE TABLE bid (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    date_time TIMESTAMP(3) NOT NULL {lateness},
    extra  VARCHAR
) WITH ('connectors' = '{bid}');
