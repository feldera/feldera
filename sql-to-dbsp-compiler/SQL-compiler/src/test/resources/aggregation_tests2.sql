CREATE TABLE varchar_tbl(
id INT,
c1 VARCHAR,
c2 VARCHAR NULL);

CREATE FUNCTION d()
RETURNS TIMESTAMP NOT NULL AS
CAST('1970-01-01 00:00:00' AS TIMESTAMP);

CREATE TABLE interval_tbl(
id INT NOT NULL,
c1 TIMESTAMP,
c2 TIMESTAMP,
c3 TIMESTAMP);

CREATE TABLE time_tbl(
id INT,
c1 TIME NOT NULL,
c2 TIME);

CREATE TABLE timestamp1_tbl(
id INT,
c1 TIMESTAMP NOT NULL,
c2 TIMESTAMP);

CREATE TABLE timestamp_tbl(
id INT,
c1 TIMESTAMP NOT NULL,
c2 TIMESTAMP);

CREATE TABLE date_tbl(
id INT,
c1 DATE NOT NULL,
c2 DATE);


CREATE MATERIALIZED VIEW atbl_charn AS SELECT
id,
CAST(c1 AS CHAR(7)) AS f_c1,
CAST(c2 AS CHAR(7)) AS f_c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW charn_argmax_distinct AS SELECT
ARG_MAX(DISTINCT f_c1, f_c2) AS c1, ARG_MAX(DISTINCT f_c2, f_c1) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmax_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT f_c1, f_c2) AS c1, ARG_MAX(DISTINCT f_c2, f_c1) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_argmax_gby AS SELECT
id, ARG_MAX(f_c1, f_c2) AS c1, ARG_MAX(f_c2, f_c1) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_argmax AS SELECT
ARG_MAX(f_c1, f_c2) AS c1, ARG_MAX(f_c2, f_c1) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmax_diff AS SELECT
ARG_MAX(f_c1||f_c2, f_c2||f_c1) AS c1, ARG_MAX(f_c2||f_c1, f_c1||f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmax_where AS SELECT
ARG_MAX(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MAX(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmax_where_gby AS SELECT
id, ARG_MAX(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MAX(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_argmax_where_gby_diff AS SELECT
id, ARG_MAX(f_c1||f_c2, f_c2||f_c1) FILTER(WHERE f_c1 LIKE '%hello  %') AS c1, ARG_MAX(f_c2||f_c1, f_c1||f_c2) FILTER(WHERE f_c1 LIKE '%hello  %') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_argmin_distinct AS SELECT
ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmin_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_argmin_gby AS SELECT
id, ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_argmin AS SELECT
ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmin_diff AS SELECT
ARG_MIN(f_c1||f_c2, f_c2||f_c1) AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmin_where AS SELECT
ARG_MIN(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_argmin_where_gby AS SELECT
id, ARG_MIN(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_argmin_where_gby_diff AS SELECT
id, ARG_MIN(f_c1||f_c2, f_c2||f_c1) FILTER(WHERE f_c1 LIKE '%hello  %') AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) FILTER(WHERE f_c1 LIKE '%hello  %') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_array_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW chan_array_agg_gby AS SELECT
id, ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_array_agg AS SELECT
ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_array_where AS SELECT
ARRAY_AGG(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARRAY_AGG(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_array_where_gby AS SELECT
id, ARRAY_AGG(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARRAY_AGG(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_count_col AS SELECT
COUNT(f_c1) AS c1, COUNT(f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_count_col_distinct AS SELECT
COUNT(DISTINCT f_c1) AS c1, COUNT(DISTINCT f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT f_c1) AS c1, COUNT(DISTINCT f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_count_col_gby AS SELECT
id, COUNT(f_c1) AS c1, COUNT(f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_count_col_where AS SELECT
COUNT(f_c1) FILTER(WHERE length(f_c1)>4) AS c1, COUNT(f_c2) FILTER(WHERE length(f_c1)>4) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_count_col_where_gby AS SELECT
id, COUNT(f_c1) FILTER(WHERE length(f_c1)>4) AS c1, COUNT(f_c2) FILTER(WHERE length(f_c1)>4) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_count AS SELECT
COUNT(*) AS count
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_count_gby AS SELECT
id, COUNT(*) AS count
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_count_where AS SELECT
COUNT(*) FILTER(WHERE length(f_c1)>4) AS count
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE length(f_c1)>4) AS count
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_max_distinct AS SELECT
MAX(DISTINCT f_c1) AS c1, MAX(DISTINCT f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_max_distinct_gby AS SELECT
id, MAX(DISTINCT f_c1) AS c1, MAX(DISTINCT f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_max_gby AS SELECT
id, MAX(f_c1) AS c1, MAX(f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_max AS SELECT
MAX(f_c1) AS c1, MAX(f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_max_where AS SELECT
MAX(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MAX(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_max_where_gby AS SELECT
id, MAX(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MAX(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_min_distinct AS SELECT
MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_min_distinct_gby AS SELECT
id, MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_min_gby AS SELECT
id, MIN(f_c1) AS c1, MIN(f_c2) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_min AS SELECT
MIN(f_c1) AS c1, MIN(f_c2) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_min_where AS SELECT
MIN(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MIN(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_min_where_gby AS SELECT
id, MIN(f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c1, MIN(f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_every AS SELECT
EVERY(f_c1 != '%hello  %') AS c1, EVERY(f_c2 LIKE '%a%') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_every_distinct AS SELECT
EVERY(DISTINCT f_c1 != '%hello  %') AS c1, EVERY(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_every_distinct_gby AS SELECT
id, EVERY(DISTINCT f_c1 != '%hello  %') AS c1, EVERY(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_every_gby AS SELECT
id, EVERY(f_c1 != '%hello  %') AS c1, EVERY(f_c2 LIKE '%a%') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_every_where AS SELECT
EVERY(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, EVERY(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_every_where_gby AS SELECT
id, EVERY(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, EVERY(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_some AS SELECT
SOME(f_c1 != '%hello  %') AS c1, SOME(f_c2 LIKE '%a%') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_some_distinct AS SELECT
SOME(DISTINCT f_c1 != '%hello  %') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_some_distinct_gby AS SELECT
id, SOME(DISTINCT f_c1 != '%hello  %') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_some_gby AS SELECT
id, SOME(f_c1 != '%hello  %') AS c1, SOME(f_c2 LIKE '%a%') AS c2
FROM atbl_charn
GROUP BY id;

CREATE MATERIALIZED VIEW charn_some_where AS SELECT
SOME(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, SOME(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
FROM atbl_charn;

CREATE MATERIALIZED VIEW charn_some_where_gby AS SELECT
id, SOME(f_c1 != '%hello  %') FILTER (WHERE f_c1 IS NOT NULL) AS c1, SOME(f_c2 LIKE '%a%') FILTER (WHERE f_c1 IS NOT NULL) AS c2
FROM atbl_charn
GROUP BY id;

CREATE LOCAL VIEW atbl_interval_months AS SELECT
id,
(c1 - c2)MONTH AS c1_minus_c2,
(c2 - c1)MONTH AS c2_minus_c1,
(c1 - c3)MONTH AS c1_minus_c3,
(c3 - c1)MONTH AS c3_minus_c1,
(c2 - c3)MONTH AS c2_minus_c3,
(c3 - c2)MONTH AS c3_minus_c2
FROM interval_tbl;

CREATE LOCAL VIEW atbl_interval_seconds AS SELECT
id,
(c1 - c2)SECOND AS c1_minus_c2,
(c2 - c1)SECOND AS c2_minus_c1,
(c1 - c3)SECOND AS c1_minus_c3,
(c3 - c1)SECOND AS c3_minus_c1,
(c2 - c3)SECOND AS c2_minus_c3,
(c3 - c2)SECOND AS c3_minus_c2
FROM interval_tbl;

CREATE LOCAL VIEW interval_arg_max AS SELECT
ARG_MAX(c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MAX(c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MAX(c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_arg_max_distinct AS SELECT
ARG_MAX(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MAX(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MAX(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_arg_max_distinct_gby AS SELECT
id,
ARG_MAX(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MAX(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MAX(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_arg_max_distinct_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_max_distinct_gby;

CREATE MATERIALIZED VIEW interval_arg_max_distinct_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_max_distinct;

CREATE LOCAL VIEW interval_arg_max_gby AS SELECT
id,
ARG_MAX(c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MAX(c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MAX(c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_arg_max_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_max_gby;

CREATE MATERIALIZED VIEW interval_arg_max_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_max;

CREATE LOCAL VIEW interval_arg_max_where AS SELECT
ARG_MAX(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
ARG_MAX(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
ARG_MAX(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_arg_max_where_gby AS SELECT
id,
ARG_MAX(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
ARG_MAX(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
ARG_MAX(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_arg_max_where_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_max_where_gby;

CREATE MATERIALIZED VIEW interval_arg_max_where_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_max_where;

CREATE LOCAL VIEW interval_arg_min AS SELECT
ARG_MIN(c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MIN(c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MIN(c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_arg_min_distinct AS SELECT
ARG_MIN(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MIN(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MIN(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_arg_min_distinct_gby AS SELECT
id,
ARG_MIN(DISTINCT c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MIN(DISTINCT c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MIN(DISTINCT c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_arg_min_distinct_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_min_distinct_gby;

CREATE MATERIALIZED VIEW interval_arg_min_distinct_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_min_distinct;

CREATE LOCAL VIEW interval_arg_min_gby AS SELECT
id,
ARG_MIN(c1_minus_c2, c2_minus_c1) AS f_c1,
ARG_MIN(c1_minus_c3, c3_minus_c1) AS f_c3,
ARG_MIN(c2_minus_c3, c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_arg_min_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_min_gby;

CREATE MATERIALIZED VIEW interval_arg_min_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_min;

CREATE LOCAL VIEW interval_arg_min_where AS SELECT
ARG_MIN(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
ARG_MIN(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
ARG_MIN(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_arg_min_where_gby AS SELECT
id,
ARG_MIN(c1_minus_c2, c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
ARG_MIN(c1_minus_c3, c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
ARG_MIN(c2_minus_c3, c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_arg_min_where_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_min_where_gby;

CREATE MATERIALIZED VIEW interval_arg_min_where_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5
FROM interval_arg_min_where;

CREATE MATERIALIZED VIEW interval_count AS SELECT
COUNT(*) AS count
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_count_gby AS SELECT
id, COUNT(*) AS count
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_count_where AS SELECT
COUNT(*) FILTER(WHERE c2_minus_c1 > c1_minus_c2) AS count
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE c2_minus_c1 > c1_minus_c2) AS count
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_count_col AS SELECT
COUNT(c1_minus_c2) AS f_c1,
COUNT(c2_minus_c1) AS f_c2,
COUNT(c1_minus_c3) AS f_c3,
COUNT(c3_minus_c1) AS f_c4,
COUNT(c2_minus_c3) AS f_c5,
COUNT(c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_count_col_distinct AS SELECT
COUNT(DISTINCT c1_minus_c2) AS f_c1,
COUNT(DISTINCT c2_minus_c1) AS f_c2,
COUNT(DISTINCT c1_minus_c3) AS f_c3,
COUNT(DISTINCT c3_minus_c1) AS f_c4,
COUNT(DISTINCT c2_minus_c3) AS f_c5,
COUNT(DISTINCT c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_count_col_distinct_gby AS SELECT
id,
COUNT(DISTINCT c1_minus_c2) AS f_c1,
COUNT(DISTINCT c2_minus_c1) AS f_c2,
COUNT(DISTINCT c1_minus_c3) AS f_c3,
COUNT(DISTINCT c3_minus_c1) AS f_c4,
COUNT(DISTINCT c2_minus_c3) AS f_c5,
COUNT(DISTINCT c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_count_col_gby AS SELECT
id,
COUNT(c1_minus_c2) AS f_c1,
COUNT(c2_minus_c1) AS f_c2,
COUNT(c1_minus_c3) AS f_c3,
COUNT(c3_minus_c1) AS f_c4,
COUNT(c2_minus_c3) AS f_c5,
COUNT(c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_count_col_where AS SELECT
COUNT(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
COUNT(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
COUNT(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
COUNT(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
COUNT(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
COUNT(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_count_col_where_gby AS SELECT
id,
COUNT(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
COUNT(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
COUNT(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
COUNT(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
COUNT(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
COUNT(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE LOCAL VIEW interval_max AS SELECT
MAX(c1_minus_c2) AS f_c1,
MAX(c2_minus_c1) AS f_c2,
MAX(c1_minus_c3) AS f_c3,
MAX(c3_minus_c1) AS f_c4,
MAX(c2_minus_c3) AS f_c5,
MAX(c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_max_distinct AS SELECT
MAX(DISTINCT c1_minus_c2) AS f_c1,
MAX(DISTINCT c2_minus_c1) AS f_c2,
MAX(DISTINCT c1_minus_c3) AS f_c3,
MAX(DISTINCT c3_minus_c1) AS f_c4,
MAX(DISTINCT c2_minus_c3) AS f_c5,
MAX(DISTINCT c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_max_distinct_gby AS SELECT
id,
MAX(DISTINCT c1_minus_c2) AS f_c1,
MAX(DISTINCT c2_minus_c1) AS f_c2,
MAX(DISTINCT c1_minus_c3) AS f_c3,
MAX(DISTINCT c3_minus_c1) AS f_c4,
MAX(DISTINCT c2_minus_c3) AS f_c5,
MAX(DISTINCT c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_max_distinct_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_max_distinct_gby;

CREATE MATERIALIZED VIEW interval_max_distinct_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_max_distinct;

CREATE LOCAL VIEW interval_max_gby AS SELECT
id,
MAX(c1_minus_c2) AS f_c1,
MAX(c2_minus_c1) AS f_c2,
MAX(c1_minus_c3) AS f_c3,
MAX(c3_minus_c1) AS f_c4,
MAX(c2_minus_c3) AS f_c5,
MAX(c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_max_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_max_gby;

CREATE MATERIALIZED VIEW interval_max_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_max;

CREATE LOCAL VIEW interval_max_where AS SELECT
MAX(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
MAX(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
MAX(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
MAX(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
MAX(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
MAX(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_max_where_gby AS SELECT
id,
MAX(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
MAX(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
MAX(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
MAX(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
MAX(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
MAX(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_max_where_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_max_where_gby;

CREATE MATERIALIZED VIEW interval_max_where_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_max_where;

CREATE LOCAL VIEW interval_min AS SELECT
MIN(c1_minus_c2) AS f_c1,
MIN(c2_minus_c1) AS f_c2,
MIN(c1_minus_c3) AS f_c3,
MIN(c3_minus_c1) AS f_c4,
MIN(c2_minus_c3) AS f_c5,
MIN(c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_min_distinct AS SELECT
MIN(DISTINCT c1_minus_c2) AS f_c1,
MIN(DISTINCT c2_minus_c1) AS f_c2,
MIN(DISTINCT c1_minus_c3) AS f_c3,
MIN(DISTINCT c3_minus_c1) AS f_c4,
MIN(DISTINCT c2_minus_c3) AS f_c5,
MIN(DISTINCT c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_min_distinct_gby AS SELECT
id,
MIN(DISTINCT c1_minus_c2) AS f_c1,
MIN(DISTINCT c2_minus_c1) AS f_c2,
MIN(DISTINCT c1_minus_c3) AS f_c3,
MIN(DISTINCT c3_minus_c1) AS f_c4,
MIN(DISTINCT c2_minus_c3) AS f_c5,
MIN(DISTINCT c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_min_distinct_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_min_distinct_gby;

CREATE MATERIALIZED VIEW interval_min_distinct_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_min_distinct;

CREATE LOCAL VIEW interval_min_gby AS SELECT
id,
MIN(c1_minus_c2) AS f_c1,
MIN(c2_minus_c1) AS f_c2,
MIN(c1_minus_c3) AS f_c3,
MIN(c3_minus_c1) AS f_c4,
MIN(c2_minus_c3) AS f_c5,
MIN(c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_min_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_min_gby;

CREATE MATERIALIZED VIEW interval_min_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_min;

CREATE LOCAL VIEW interval_min_where AS SELECT
MIN(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
MIN(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
MIN(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
MIN(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
MIN(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
MIN(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
FROM atbl_interval_seconds;

CREATE LOCAL VIEW interval_min_where_gby AS SELECT
id,
MIN(c1_minus_c2) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c1,
MIN(c2_minus_c1) FILTER(WHERE c1_minus_c2 > c2_minus_c1) AS f_c2,
MIN(c1_minus_c3) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c3,
MIN(c3_minus_c1) FILTER(WHERE c1_minus_c3 > c3_minus_c1) AS f_c4,
MIN(c2_minus_c3) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c5,
MIN(c3_minus_c2) FILTER(WHERE c2_minus_c3 > c3_minus_c2) AS f_c6
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_min_where_gby_seconds AS SELECT
id,
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_min_where_gby;

CREATE MATERIALIZED VIEW interval_min_where_seconds AS SELECT
TIMESTAMPDIFF(SECOND, d(), d() + f_c1) AS m_c1,
TIMESTAMPDIFF(SECOND, d(), d() + f_c2) AS m_c2,
TIMESTAMPDIFF(SECOND, d(), d() + f_c3) AS m_c3,
TIMESTAMPDIFF(SECOND, d(), d() + f_c4) AS m_c4,
TIMESTAMPDIFF(SECOND, d(), d() + f_c5) AS m_c5,
TIMESTAMPDIFF(SECOND, d(), d() + f_c6) AS m_c6
FROM interval_min_where;

CREATE MATERIALIZED VIEW interval_every AS SELECT
EVERY(c1_minus_c2 > c2_minus_c1) AS f_c1,
EVERY(c1_minus_c3 < c3_minus_c1) AS f_c3,
EVERY(c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_every_distinct AS SELECT
EVERY(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
EVERY(DISTINCT c1_minus_c3 < c3_minus_c1) AS f_c3,
EVERY(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_every_distinct_gby AS SELECT
id,
EVERY(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
EVERY(DISTINCT c1_minus_c3 < c3_minus_c1) AS f_c3,
EVERY(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_every_gby AS SELECT
id,
EVERY(c1_minus_c2 > c2_minus_c1) AS f_c1,
EVERY(c1_minus_c3 < c3_minus_c1) AS f_c3,
EVERY(c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_every_where AS SELECT
EVERY(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c1,
EVERY(c1_minus_c3 < c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c3,
EVERY(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c5
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_every_where_gby AS SELECT
id,
EVERY(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c1,
EVERY(c1_minus_c3 < c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c3,
EVERY(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_some AS SELECT
SOME(c1_minus_c2 > c2_minus_c1) AS f_c1,
SOME(c1_minus_c3 > c3_minus_c1) AS f_c3,
SOME(c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_some_distinct AS SELECT
SOME(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
SOME(DISTINCT c1_minus_c3 > c3_minus_c1) AS f_c3,
SOME(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_some_distinct_gby AS SELECT
id,
SOME(DISTINCT c1_minus_c2 > c2_minus_c1) AS f_c1,
SOME(DISTINCT c1_minus_c3 > c3_minus_c1) AS f_c3,
SOME(DISTINCT c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_some_gby AS SELECT
id,
SOME(c1_minus_c2 > c2_minus_c1) AS f_c1,
SOME(c1_minus_c3 > c3_minus_c1) AS f_c3,
SOME(c2_minus_c3 > c3_minus_c2) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW interval_some_where AS SELECT
SOME(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c1,
SOME(c1_minus_c3 > c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c3,
SOME(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c5
FROM atbl_interval_seconds;

CREATE MATERIALIZED VIEW interval_some_where_gby AS SELECT
id,
SOME(c1_minus_c2 > c2_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c1,
SOME(c1_minus_c3 > c3_minus_c1) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c3,
SOME(c2_minus_c3 > c3_minus_c2) FILTER(WHERE c1_minus_c2::BIGINT > 318185100) AS f_c5
FROM atbl_interval_seconds
GROUP BY id;

CREATE MATERIALIZED VIEW time_arg_max_distinct AS SELECT
ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_arg_max_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_arg_max_gby AS SELECT
id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_arg_max AS SELECT
ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_arg_max_where AS SELECT
ARG_MAX(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_arg_max_where_gby AS SELECT
id, ARG_MAX(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_arg_min_distinct AS SELECT
ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_arg_min_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_arg_min_gby AS SELECT
id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_arg_min AS SELECT
ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_arg_min_where AS SELECT
ARG_MIN(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_arg_min_where_gby AS SELECT
id, ARG_MIN(c1, c2) FILTER(WHERE c1 > '08:30:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '08:30:00') AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_array_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_array_agg_gby AS SELECT
id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_array_agg AS SELECT
ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_array_where AS SELECT
ARRAY_AGG(c1) FILTER(WHERE c1 > '08:30:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '08:30:00') AS f_c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_array_where_gby AS SELECT
id, ARRAY_AGG(c1) FILTER(WHERE c1 > '08:30:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '08:30:00') AS f_c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_count_col AS SELECT
COUNT(c1) AS c1, COUNT(c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_count_col_distinct AS SELECT
COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_count_col_gby AS SELECT
id, COUNT(c1) AS c1, COUNT(c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_count_col_where AS SELECT
COUNT(c1) FILTER(WHERE c1 > '08:30:00') AS c1, COUNT(c2) FILTER(WHERE c1 > '08:30:00') AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_count_col_where_gby AS SELECT
id, COUNT(c1) FILTER(WHERE c1 > '08:30:00') AS c1, COUNT(c2) FILTER(WHERE c1 > '08:30:00') AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_count AS SELECT
COUNT(*) AS count
FROM time_tbl;

CREATE MATERIALIZED VIEW time_count_gby AS SELECT
id, COUNT(*) AS count
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_count_where AS SELECT
COUNT(*) FILTER(WHERE c1 > '08:30:00') AS count
FROM time_tbl;

CREATE MATERIALIZED VIEW time_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE c1 > '08:30:00') AS count
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_max_distinct AS SELECT
MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_max_distinct_gby AS SELECT
id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_max_gby AS SELECT
id, MAX(c1) AS c1, MAX(c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_max AS SELECT
MAX(c1) AS c1, MAX(c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_max_where AS SELECT
MAX(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_max_where_gby AS SELECT
id, MAX(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_min_distinct AS SELECT
MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_min_distinct_gby AS SELECT
id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_min_gby AS SELECT
id, MIN(c1) AS c1, MIN(c2) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_min AS SELECT
MIN(c1) AS c1, MIN(c2) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_min_where AS SELECT
MIN(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_min_where_gby AS SELECT
id, MIN(c1) FILTER (WHERE c1 > '08:30:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '08:30:00') AS f_c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_every AS SELECT
EVERY(c1 > '08:30:00') AS c1, EVERY(c2 > '12:45:00') AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_every_distinct AS SELECT
EVERY(DISTINCT c1 > '08:30:00') AS c1, EVERY(DISTINCT c2 > '12:45:00') AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_every_distinct_gby AS SELECT
id, EVERY(DISTINCT c1 > '08:30:00') AS c1, EVERY(DISTINCT c2 > '12:45:00') AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_every_gby AS SELECT
id, EVERY(c1 > '08:30:00') AS c1, EVERY(c2 > '12:45:00') AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_every_where AS SELECT
EVERY(c1 > '08:30:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_every_where_gby AS SELECT
id, EVERY(c1 > '08:30:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_some AS SELECT
SOME(c1 > '08:30:00') AS c1, SOME(c2 > '12:45:00') AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_some_distinct AS SELECT
SOME(DISTINCT c1 > '08:30:00') AS c1, SOME(DISTINCT c2 > '12:45:00') AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_some_distinct_gby AS SELECT
id, SOME(DISTINCT c1 > '08:30:00') AS c1, SOME(DISTINCT c2 > '12:45:00') AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_some_gby AS SELECT
id, SOME(c1 > '08:30:00') AS c1, SOME(c2 > '12:45:00') AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW time_some_where AS SELECT
SOME(c1 > '08:30:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM time_tbl;

CREATE MATERIALIZED VIEW time_some_where_gby AS SELECT
id, SOME(c1 > '08:30:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM time_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_max_distinct AS SELECT
ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_arg_max_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_max_gby AS SELECT
id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_max AS SELECT
ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_arg_max_where AS SELECT
ARG_MAX(c1, c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_arg_max_where1_gby AS SELECT
id, ARG_MAX(c1, c2) FILTER(WHERE c1 < '2020-06-21 14:00:00') AS f_c1, ARG_MAX(c2, c1) FILTER(WHERE c2 > '2014-11-05 16:30:00') AS f_c2
FROM timestamp1_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_max_where_gby AS SELECT
id, ARG_MAX(c1, c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_min1 AS SELECT
ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM timestamp1_tbl;

CREATE MATERIALIZED VIEW timestamp_arg_min_distinct AS SELECT
ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_arg_min_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_min_gby AS SELECT
id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_min AS SELECT
ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_arg_min_where AS SELECT
ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_arg_min_where1_gby AS SELECT
id, ARG_MIN(c1, c2) FILTER(WHERE c1 < '2020-06-21 14:00:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c2 > '2014-11-05 16:30:00') AS c2
FROM timestamp1_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_arg_min_where_gby AS SELECT
id, ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_array_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_array_agg_gby AS SELECT
id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_array_agg AS SELECT
ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_array_where AS SELECT
ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_array_where_gby AS SELECT
id, ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS f_c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_count_col AS SELECT
COUNT(c1) AS c1, COUNT(c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_count_col_distinct AS SELECT
COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_count_col_gby AS SELECT
id, COUNT(c1) AS c1, COUNT(c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_count_col_where AS SELECT
COUNT(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, COUNT(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_count_col_where_gby AS SELECT
id, COUNT(c1) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c1, COUNT(c2) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_count AS SELECT
COUNT(*) AS count
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_count_gby AS SELECT
id, COUNT(*) AS count
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_count_where AS SELECT
COUNT(*) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS count
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE c1 > '2014-11-05 08:27:00') AS count
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_every AS SELECT
EVERY(c1 > '2014-11-05 08:27:00') AS c1, EVERY(c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_every_distinct AS SELECT
EVERY(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, EVERY(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_every_distinct_gby AS SELECT
id, EVERY(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, EVERY(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_every_gby AS SELECT
id, EVERY(c1 > '2014-11-05 08:27:00') AS c1, EVERY(c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_every_where AS SELECT
EVERY(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_every_where1_gby AS SELECT
id, EVERY(c1 < '2020-06-21 14:00:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '2014-11-05 16:30:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM timestamp1_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_every_where_gby AS SELECT
id, EVERY(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_max_distinct AS SELECT
MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_max_distinct_gby AS SELECT
id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_max_gby AS SELECT
id, MAX(c1) AS c1, MAX(c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_max AS SELECT
MAX(c1) AS c1, MAX(c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_max_where AS SELECT
MAX(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_max_where1_gby AS SELECT
id, MAX(c1) FILTER (WHERE c1 < '2020-06-21 14:00:00') AS f_c1, MAX(c2) FILTER (WHERE c2 > '2014-11-05 16:30:00') AS f_c2
FROM timestamp1_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_max_where_gby AS SELECT
id, MAX(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_min1 AS SELECT
MIN(c1) AS c1, MIN(c2) AS c2
FROM timestamp1_tbl;

CREATE MATERIALIZED VIEW timestamp_min_distinct AS SELECT
MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_min_distinct_gby AS SELECT
id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_min_gby AS SELECT
id, MIN(c1) AS c1, MIN(c2) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_min AS SELECT
MIN(c1) AS c1, MIN(c2) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_min_where AS SELECT
MIN(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_min_where1_gby AS SELECT
id, MIN(c1) FILTER (WHERE c1 < '2020-06-21 14:00:00') AS f_c1, MIN(c2) FILTER (WHERE c2 > '2014-11-05 16:30:00') AS f_c2
FROM timestamp1_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_min_where_gby AS SELECT
id, MIN(c1) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05 08:27:00') AS f_c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_some AS SELECT
SOME(c1 > '2014-11-05 08:27:00') AS c1, SOME(c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_some_distinct AS SELECT
SOME(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, SOME(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_some_distinct_gby AS SELECT
id, SOME(DISTINCT c1 > '2014-11-05 08:27:00') AS c1, SOME(DISTINCT c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_some_gby AS SELECT
id, SOME(c1 > '2014-11-05 08:27:00') AS c1, SOME(c2 > '2024-12-05 12:45:00') AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_some_where AS SELECT
SOME(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM timestamp_tbl;

CREATE MATERIALIZED VIEW timestamp_some_where1_gby AS SELECT
id, SOME(c1 < '2020-06-21 14:00:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '2014-11-05 16:30:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM timestamp1_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW timestamp_some_where_gby AS SELECT
id, SOME(c1 > '2014-11-05 08:27:00') FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > '2024-12-05 12:45:00') FILTER(WHERE c2 IS NOT NULL) AS c2
FROM timestamp_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_arg_max_distinct AS SELECT
ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_arg_max_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_arg_max_gby AS SELECT
id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_arg_max AS SELECT
ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_arg_max_where AS SELECT
ARG_MAX(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_arg_max_where_gby AS SELECT
id, ARG_MAX(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_arg_min_distinct AS SELECT
ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_arg_min_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_arg_min_gby AS SELECT
id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_arg_min AS SELECT
ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_arg_min_where AS SELECT
ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_arg_min_where_gby AS SELECT
id, ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_array_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_array_agg_gby AS SELECT
id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_array_agg AS SELECT
ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_array_where AS SELECT
ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > '2014-11-05') AS f_c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_array_where_gby AS SELECT
id, ARRAY_AGG(c1) FILTER(WHERE c1 > '2014-11-05') AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c2 > '2014-11-05') AS f_c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_count_col AS SELECT
COUNT(c1) AS c1, COUNT(c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_count_col_distinct AS SELECT
COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_count_col_gby AS SELECT
id, COUNT(c1) AS c1, COUNT(c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_count_col_where AS SELECT
COUNT(c1) FILTER(WHERE c1 > '2014-11-05') AS c1, COUNT(c2) FILTER(WHERE c1 > '2014-11-05') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_count_col_where_gby AS SELECT
id, COUNT(c1) FILTER(WHERE c1 > '2014-11-05') AS c1, COUNT(c2) FILTER(WHERE c1 > '2014-11-05') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_count AS SELECT
COUNT(*) AS count
FROM date_tbl;

CREATE MATERIALIZED VIEW date_count_gby AS SELECT
id, COUNT(*) AS count
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_count_where AS SELECT
COUNT(*) FILTER(WHERE c1 > '2014-11-05') AS count
FROM date_tbl;

CREATE MATERIALIZED VIEW date_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE c1 > '2014-11-05') AS count
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_every AS SELECT
EVERY(c1 > '1969-03-17') AS c1, EVERY(c2 < '2024-12-05') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_every_distinct AS SELECT
EVERY(DISTINCT c1 > '1969-03-17') AS c1, EVERY(DISTINCT c2 < '2024-12-05') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_every_distinct_gby AS SELECT
id, EVERY(DISTINCT c1 > '1969-03-17') AS c1, EVERY(DISTINCT c2 < '2024-12-05') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_every_gby AS SELECT
id, EVERY(c1 > '1969-03-17') AS c1, EVERY(c2 < '2024-12-05') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_every_where AS SELECT
EVERY(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, EVERY(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_every_where_gby AS SELECT
id, EVERY(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, EVERY(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_some AS SELECT
SOME(c1 > '1969-03-17') AS c1, SOME(c2 < '2024-12-05') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_some_distinct AS SELECT
SOME(DISTINCT c1 > '1969-03-17') AS c1, SOME(DISTINCT c2 < '2024-12-05') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_some_distinct_gby AS SELECT
id, SOME(DISTINCT c1 > '1969-03-17') AS c1, SOME(DISTINCT c2 < '2024-12-05') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_some_gby AS SELECT
id, SOME(c1 > '1969-03-17') AS c1, SOME(c2 < '2024-12-05') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_some_where AS SELECT
SOME(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, SOME(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_some_where_gby AS SELECT
id, SOME(c1 > '1969-03-17') FILTER(WHERE c1 > '2014-11-05') AS c1, SOME(c2 < '2024-12-05') FILTER(WHERE c2 > '2015-09-07') AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_max_distinct AS SELECT
MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_max_distinct_gby AS SELECT
id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_max_gby AS SELECT
id, MAX(c1) AS c1, MAX(c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_max AS SELECT
MAX(c1) AS c1, MAX(c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_max_where AS SELECT
MAX(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_max_where_gby AS SELECT
id, MAX(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_min_distinct AS SELECT
MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_min_distinct_gby AS SELECT
id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_min_gby AS SELECT
id, MIN(c1) AS c1, MIN(c2) AS c2
FROM date_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW date_min AS SELECT
MIN(c1) AS c1, MIN(c2) AS c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_min_where AS SELECT
MIN(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
FROM date_tbl;

CREATE MATERIALIZED VIEW date_min_where_gby AS SELECT
id, MIN(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
FROM date_tbl
GROUP BY id;
