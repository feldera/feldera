CREATE TABLE person (
    id BIGINT,
    name VARCHAR,
    emailAddress VARCHAR,
    creditCard VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP(3) NOT NULL {lateness},
    extra  VARCHAR
) WITH ('connectors' = '[
    {{
        "format": {{
            "name": "csv",
            "config": {{}}
        }},
        "transport": {{
            "name": "nexmark",
            "config": {{
                "table": "person",
                "options": {{
                    "events": {events},
                    "threads": {cores},
                    "batch_size_per_thread": 1000,
                    "max_step_size_per_thread": 10000
                }}
            }}
        }}
    }}
]');
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
) WITH ('connectors' = '[
    {{
        "format": {{
            "name": "csv",
            "config": {{}}
        }},
        "transport": {{
            "name": "nexmark",
            "config": {{
                "table": "auction"
            }}
        }}
    }}
]');
CREATE TABLE bid (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    date_time TIMESTAMP(3) NOT NULL {lateness},
    extra  VARCHAR
) WITH ('connectors' = '[
    {{
        "format": {{
            "name": "csv",
            "config": {{}}
        }},
        "transport": {{
            "name": "nexmark",
            "config": {{
                "table": "bid"
            }}
        }}
    }}
]');
CREATE TABLE side_input (
  date_time TIMESTAMP,
  key BIGINT,
  value VARCHAR
) WITH ('connectors' = '[{{
  "transport": {{
      "name": "datagen",
      "config": {{
        "plan": [
          {{
            "limit": 100,
            "fields": {{
              "date_time": {{ "range": [1724444408000, 4102444800000] }},
              "key": {{ "range": [0, 100] }}
            }}
          }}
        ]
      }}
    }}
}}]');
