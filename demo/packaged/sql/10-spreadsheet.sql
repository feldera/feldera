-- Tutorial: Building a spreadsheet using Feldera (spreadsheet)
--
-- A copy of the billion-cell spreadsheet demo from https://github.com/feldera/techdemo-spreadsheet.
--

-- Given a cell value as a formula (e.g., =A0+B0), and a context with cell values
-- referenced in the formula, returns the computed value of the cell
create function cell_value(cell varchar(64), mentions_ids bigint array, mentions_values varchar(64) array) returns varchar(64);

-- Given a cell value e.g., =A0+B0, returns an array of cell ids that were mentioned in the formula
create function mentions(cell varchar(64)) returns bigint array;

-- Forward declaration of spreadsheet view
declare recursive view spreadsheet_view (
     id bigint not null,
     background integer not null,
     raw_value varchar(64) not null,
     computed_value varchar(64)
);

-- Raw spreadsheet cell data coming from backend/user, updates
-- are inserted as new entries with newer timestamps
create table spreadsheet_data (
     id bigint not null,
     ip varchar(45) not null,
     ts timestamp not null,
     raw_value varchar(64) not null,
     background integer not null
) with (
      'materialized' = 'true',
      'connectors' = '[{
        "name": "data",
        "transport": {
            "name": "datagen",
            "config": {
                "workers": 1,
                "plan": [{
                    "limit": 23,
                    "fields": {
                        "id": { "values": [1039999974, 0, 1, 2, 12, 14, 40, 66, 92, 118, 170, 196, 222, 13, 65, 91, 117, 15, 41, 67, 93, 119, 39, 144] },
                        "ip": { "values": ["0"] },
                        "raw_value": { "values": ["42", "=A39999999", "=A0", "=A0+B0", "Reference", "Functions", "=ABS(-1)", "=AVERAGE(1,2,3,1,2,3)", "={1,2,3}+{1,2,3}", "=SUM(1,2,3)", "=PRODUCT(ABS(1),2*1, 3,4*1)", "=RIGHT(\"apple\", 3)", "=LEFT(\"apple\", 3)", "Logic", "=2>=1", "=OR(1>1,1<>1)", "=AND(\"test\",\"True\", 1, true)", "Datetime", "2019-03-01T02:00:00.000Z", "2019-08-30T02:00:00.000Z", "=DAYS(P1, P2)", "=P1+5", "=XOR(0,1)", "=IF(TRUE,1,0)"] },
                        "background": { "strategy": "uniform", "range": [0, 1] }
                    }
                }]
            }
        }
    }]'
      );

-- Get the latest cell value for the spreadsheet.
-- (By finding the one with the highest `ts` for a given `id`)
create view latest_cells as with
       max_ts_per_cell as (
           select
               id,
               max(ts) as max_ts
           from
               spreadsheet_data
           group by
               id
       )
   select
       s.id,
       s.raw_value,
       s.background,
       -- The append with null is silly but crucial to ensure that the
       -- cross join in `latest_cells_with_mention` returns all cells
       -- not just those that reference another cell
       ARRAY_APPEND(mentions(s.raw_value), null) as mentioned_cell_ids
   from
       spreadsheet_data s
           join max_ts_per_cell mt on s.id = mt.id and s.ts = mt.max_ts;

-- List all mentioned ids per latest cell
create view latest_cells_with_mentions as
select
    s.id,
    s.raw_value,
    s.background,
    m.mentioned_id
from
    latest_cells s, unnest(s.mentioned_cell_ids) as m(mentioned_id);

-- Like latest_cells_with_mentions, but enrich it with values of mentioned cells
create local view mentions_with_values as
select
    m.id,
    m.raw_value,
    m.background,
    m.mentioned_id,
    sv.computed_value as mentioned_value
from
    latest_cells_with_mentions m
        left join
    spreadsheet_view sv on m.mentioned_id = sv.id;

-- We aggregate mentioned values and ids back into arrays
create local view mentions_aggregated as
select
    id,
    raw_value,
    background,
    ARRAY_AGG(mentioned_id) as mentions_ids,
    ARRAY_AGG(mentioned_value) as mentions_values
from
    mentions_with_values
group by
    id,
    raw_value,
    background;

-- Calculate the final spreadsheet by executing the UDF for the formula
create materialized view spreadsheet_view as
select
    id,
    background,
    raw_value,
    cell_value(raw_value, mentions_ids, mentions_values) AS computed_value
from
    mentions_aggregated;

-- Figure out which IPs currently reached their API limit
create materialized view api_limit_reached as
select
    ip
from
    spreadsheet_data
where
    ts >= NOW() - INTERVAL 60 MINUTES
group by
    ip
having
    count(*) > 100;

-- Compute statistics
create materialized view spreadsheet_statistics as
with filled_total as (
    select
        count(distinct id) as filled_total
    from
        spreadsheet_data
),
filled_this_hour as (
    select
        count(*) as filled_this_hour
    from
        spreadsheet_data
    where
        ts >= NOW() - INTERVAL 1 HOUR
),
filled_today as (
    select
        count(*) as filled_today
    from
        spreadsheet_data
    where
        ts >= NOW() - INTERVAL 1 DAY
),
filled_this_week as (
    select
        count(*) as filled_this_week
    from
        spreadsheet_data
    where
        ts >= NOW() - INTERVAL 1 WEEK
),
currently_active_users as (
    select
        count(distinct ip) as currently_active_users
    from
        spreadsheet_data
    where
        ts >= NOW() - INTERVAL 5 MINUTE
)
select
    (select filled_total from filled_total) as filled_total,
    (select filled_this_hour from filled_this_hour) as filled_this_hour,
    (select filled_today from filled_today) as filled_today,
    (select filled_this_week from filled_this_week) as filled_this_week,
    (select currently_active_users from currently_active_users) as currently_active_users;
