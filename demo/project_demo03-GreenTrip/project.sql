CREATE TABLE green_tripdata
(
    lpep_pickup_datetime TIMESTAMP NOT NULL,
    lpep_dropoff_datetime TIMESTAMP NOT NULL,
    pickup_location_id BIGINT NOT NULL,
    dropoff_location_id BIGINT NOT NULL,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION
);

CREATE VIEW FEATURES as SELECT
    *,
    COUNT(*) OVER(
      PARTITION BY  pickup_location_id
      ORDER BY extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) )
      -- 1 hour is 3600  seconds
      RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip,
    AVG(fare_amount) OVER(
      PARTITION BY  pickup_location_id
      ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) )
      -- 1 hour is 3600  seconds
      RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS mean_fare_window_1h_pickup_zip,
    COUNT(*) OVER(
       PARTITION BY  dropoff_location_id
       ORDER BY  extract (EPOCH from  CAST (lpep_dropoff_datetime AS TIMESTAMP) )
       -- 0.5 hour is 1800  seconds
       RANGE BETWEEN 1800  PRECEDING AND 1 PRECEDING ) AS count_trips_window_30m_dropoff_zip,
    case when extract (ISODOW from  CAST (lpep_dropoff_datetime AS TIMESTAMP)) > 5 then 1 else 0 end as dropoff_is_weekend
FROM green_tripdata;
