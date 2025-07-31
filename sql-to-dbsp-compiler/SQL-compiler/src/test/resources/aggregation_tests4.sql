CREATE TABLE int0_tbl(
id INT NOT NULL,
c1 TINYINT,
c2 TINYINT NOT NULL,
c3 INT2,
c4 INT2 NOT NULL,
c5 INT,
c6 INT NOT NULL,
c7 BIGINT,
c8 BIGINT NOT NULL);

CREATE TABLE stddev_tbl(
id INT NOT NULL,
c1 TINYINT,
c2 TINYINT NOT NULL,
c3 INT2,
c4 INT2 NOT NULL,
c5 INT,
c6 INT NOT NULL,
c7 BIGINT,
c8 BIGINT NOT NULL);

CREATE TABLE int_tbl(
id INT, c1 INT, c2 INT NOT NULL);

CREATE TABLE varchar_tbl(
id INT,
c1 VARCHAR,
c2 VARCHAR NULL);

CREATE TABLE array_tbl(
id INT,
c1 INT ARRAY NOT NULL,
c2 INT ARRAY,
c3 MAP<VARCHAR, INT> ARRAY);

CREATE TABLE uuid_tbl(
id INT,
c1 UUID,
c2 UUID);

CREATE TABLE map_tbl(
id INT,
c1 MAP<VARCHAR, INT> NOT NULL,
c2 MAP<VARCHAR, INT>);


CREATE MATERIALIZED VIEW atbl_charn AS SELECT
id,
CAST(c1 AS CHAR(7)) AS f_c1,
CAST(c2 AS CHAR(7)) AS f_c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW int_array_agg_where_gby AS SELECT
id, ARRAY_AGG(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3, ARRAY_AGG(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4, ARRAY_AGG(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5, ARRAY_AGG(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6,  ARRAY_AGG(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7,  ARRAY_AGG(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
FROM int0_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW int_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
FROM int0_tbl;

CREATE MATERIALIZED VIEW int_array_agg_distinct_groupby AS SELECT
id, ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
FROM int0_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW int_array_agg_gby AS SELECT
id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
FROM int0_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW int_array_agg AS SELECT
ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
FROM int0_tbl;

CREATE MATERIALIZED VIEW int_array_agg_where AS SELECT
ARRAY_AGG(c1) FILTER(WHERE (c5+C6)> 3) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE (c5+C6)> 3) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE (c5+C6)> 3) AS f_c3, ARRAY_AGG(c4) FILTER(WHERE (c5+C6)> 3) AS f_c4, ARRAY_AGG(c5) FILTER(WHERE (c5+C6)> 3) AS f_c5, ARRAY_AGG(c6) FILTER(WHERE (c5+C6)> 3) AS f_c6,  ARRAY_AGG(c7) FILTER(WHERE (c5+C6)> 3) AS f_c7,  ARRAY_AGG(c8) FILTER(WHERE (c5+C6)> 3) AS f_c8
FROM int0_tbl;

CREATE MATERIALIZED VIEW array_arg_max_distinct AS SELECT
ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arg_max_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arg_max_gby AS SELECT
id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arg_max AS SELECT
ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arg_max_where AS SELECT
ARG_MAX(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arg_max_where_gby AS SELECT
id, ARG_MAX(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arg_min_distinct AS SELECT
ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arg_min_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arg_min_gby AS SELECT
id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arg_min AS SELECT
ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arg_min_where AS SELECT
ARG_MIN(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arg_min_where_gby AS SELECT
id, ARG_MIN(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arr_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arr_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arr_agg_gby AS SELECT
id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_arr_agg AS SELECT
ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arr_agg_where AS SELECT
ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE c1 < c2) AS f_c3
FROM array_tbl;

CREATE MATERIALIZED VIEW array_arr_agg_where_gby AS SELECT
id, ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE c1 < c2) AS f_c3
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_count AS SELECT
COUNT(*) AS count
FROM array_tbl;

CREATE MATERIALIZED VIEW array_count_gby AS SELECT
id, COUNT(*) AS count
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_count_where AS SELECT
COUNT(*) FILTER(WHERE c1 < c2) AS count
FROM array_tbl;

CREATE MATERIALIZED VIEW array_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE c1 < c2) AS count
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_count_col AS SELECT
COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3
FROM array_tbl;

CREATE MATERIALIZED VIEW array_count_col_distinct AS SELECT
COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3
FROM array_tbl;

CREATE MATERIALIZED VIEW array_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_count_col_gby AS SELECT
id, COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_count_col_where AS SELECT
COUNT(c1) FILTER(WHERE c1 < c2) AS c1, COUNT(c2) FILTER(WHERE c1 < c2) AS c2, COUNT(c3) FILTER(WHERE c1 < c2) AS c3
FROM array_tbl;

CREATE MATERIALIZED VIEW array_count_col_where_gby AS SELECT
id, COUNT(c1) FILTER(WHERE c1 < c2) AS c1, COUNT(c2) FILTER(WHERE c1 < c2) AS c2, COUNT(c3) FILTER(WHERE c1 < c2) AS c3
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_every AS SELECT
EVERY(c1 > c2) AS c1, EVERY(c2 > c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_every_distinct AS SELECT
EVERY(DISTINCT c1 > c2) AS c1, EVERY(DISTINCT c2 > c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_every_distinct_gby AS SELECT
id, EVERY(DISTINCT c1 > c2) AS c1, EVERY(DISTINCT c2 > c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_every_gby AS SELECT
id, EVERY(c1 > c2) AS c1, EVERY(c2 > c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_every_where AS SELECT
EVERY(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_every_where_gby AS SELECT
id, EVERY(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_some AS SELECT
SOME(c1 > c2) AS c1, SOME(c2 > c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_some_distinct AS SELECT
SOME(DISTINCT c1 > c2) AS c1, SOME(DISTINCT c2 > c1) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_some_distinct_gby AS SELECT
id, SOME(DISTINCT c1 > c2) AS c1, SOME(DISTINCT c2 > c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_some_gby AS SELECT
id, SOME(c1 > c2) AS c1, SOME(c2 > c1) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_some_where AS SELECT
SOME(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_some_where_gby AS SELECT
id, SOME(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_max_distinct AS SELECT
MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_max_distinct_gby AS SELECT
id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_max_gby AS SELECT
id, MAX(c1) AS c1, MAX(c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_max AS SELECT
MAX(c1) AS c1, MAX(c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_max_where AS SELECT
MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_max_where_gby AS SELECT
id, MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_min_distinct AS SELECT
MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_min_distinct_gby AS SELECT
id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_min_gby AS SELECT
id, MIN(c1) AS c1, MIN(c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW array_min AS SELECT
MIN(c1) AS c1, MIN(c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_min_where AS SELECT
MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl;

CREATE MATERIALIZED VIEW array_min_where_gby AS SELECT
id, MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
FROM array_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_max_distinct AS SELECT
ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_max_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_max_gby AS SELECT
id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_max AS SELECT
ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_max_diff AS SELECT
ARG_MAX(c1||c2, c2||c1) AS c1, ARG_MAX(c2||c1, c1||c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_max_where AS SELECT
ARG_MAX(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MAX(c2, c1) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_max_where_gby AS SELECT
id, ARG_MAX(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MAX(c2, c1) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_max_where_gby_diff AS SELECT
id, ARG_MAX(c1||c2, c2||c1) FILTER(WHERE len(c1)>4) AS c1, ARG_MAX(c2||c1, c1||c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_min_distinct AS SELECT
ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_min_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_min_gby AS SELECT
id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_min AS SELECT
ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_min_diff AS SELECT
ARG_MIN(c1||c2, c2||c1) AS c1, ARG_MIN(c2||c1, c1||c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_min_where AS SELECT
ARG_MIN(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MIN(c2, c1) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_arg_min_where_gby AS SELECT
id, ARG_MIN(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MIN(c2, c1) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_arg_min_where_gby_diff AS SELECT
id, ARG_MIN(c1||c2, c2||c1) FILTER(WHERE len(c1)>4) AS c1, ARG_MIN(c2||c1, c1||c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_array_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_array_agg_gby AS SELECT
id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_array_agg AS SELECT
ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_array_where AS SELECT
ARRAY_AGG(c1) FILTER(WHERE len(c2)>4) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE len(c2)>4) AS f_c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_array_where_gby AS SELECT
id, ARRAY_AGG(c1) FILTER(WHERE len(c2)>4) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE len(c2)>4) AS f_c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_count AS SELECT
COUNT(*) AS count
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_count_gby AS SELECT
id, COUNT(*) AS count
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_count_where AS SELECT
COUNT(*) FILTER(WHERE len(c1)>4) AS count
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE len(c1)>4) AS count
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_count_col AS SELECT
COUNT(c1) AS c1, COUNT(c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_count_col_distinct AS SELECT
COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_count_col_gby AS SELECT
id, COUNT(c1) AS c1, COUNT(c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_count_col_where AS SELECT
COUNT(c1) FILTER(WHERE len(c1)>4) AS c1, COUNT(c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_coun_col_where_gby AS SELECT
id, COUNT(c1) FILTER(WHERE len(c1)>4) AS c1, COUNT(c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_every AS SELECT
EVERY(c1 != '%hello%') AS c1, EVERY(c2 LIKE '%a%') AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_every_distinct AS SELECT
EVERY(DISTINCT c1 != '%hello%') AS c1, EVERY(DISTINCT c2 LIKE '%a%') AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_every_distinct_gby AS SELECT
id, EVERY(DISTINCT c1 != '%hello%') AS c1, EVERY(DISTINCT c2 LIKE '%a%') AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_every_gby AS SELECT
id, EVERY(c1 != '%hello%') AS c1, EVERY(c2 LIKE '%a%') AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_every_where AS SELECT
EVERY(c1 != '%hello%') FILTER(WHERE len(c2)=4) AS c1, EVERY(c2 LIKE '%a%') FILTER(WHERE len(c2)=4) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_every_where_gby AS SELECT
id, EVERY(c1 != '%hello%') FILTER(WHERE len(c2)=4) AS c1, EVERY(c2 LIKE '%a%') FILTER(WHERE len(c2)=4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_max_distinct AS SELECT
MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_max_distinct_gby AS SELECT
id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_max_gby AS SELECT
id, MAX(c1) AS c1, MAX(c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_max AS SELECT
MAX(c1) AS c1, MAX(c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_max_where AS SELECT
MAX(c1) FILTER(WHERE len(c1)>4) AS c1, MAX(c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_max_where_gby AS SELECT
id, MAX(c1) FILTER(WHERE len(c1)>4) AS c1, MAX(c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_min_distinct AS SELECT
MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_min_distinct_gby AS SELECT
id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_min_gby AS SELECT
id, MIN(c1) AS c1, MIN(c2) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_min AS SELECT
MIN(c1) AS c1, MIN(c2) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_min_where AS SELECT
MIN(c1) FILTER(WHERE len(c1)>4) AS c1, MIN(c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_min_where_gby AS SELECT
id, MIN(c1) FILTER(WHERE len(c1)>4) AS c1, MIN(c2) FILTER(WHERE len(c1)>4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_some AS SELECT
SOME(c1 != '%hello%') AS c1, SOME(c2 LIKE '%a%') AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_some_distinct AS SELECT
SOME(DISTINCT c1 != '%hello%') AS c1, SOME(DISTINCT c2 LIKE '%a%') AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_some_distinct_gby AS SELECT
id, SOME(DISTINCT c1 != '%hello%') AS c1, SOME(DISTINCT c2 LIKE '%a%') AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_some_gby AS SELECT
id, SOME(c1 != '%hello%') AS c1, SOME(c2 LIKE '%a%') AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW varchar_some_where AS SELECT
SOME(c1 != '%hello%') FILTER(WHERE len(c2)=4) AS c1, SOME(c2 LIKE '%a%') FILTER(WHERE len(c2)=4) AS c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varchar_some_where_gby AS SELECT
id, SOME(c1 != '%hello%') FILTER(WHERE len(c2)=4) AS c1, SOME(c2 LIKE '%a%') FILTER(WHERE len(c2)=4) AS c2
FROM varchar_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW atbl_varcharn AS SELECT
id,
CAST(c1 AS VARCHAR(5)) AS f_c1,
CAST(c2 AS VARCHAR(5)) AS f_c2
FROM varchar_tbl;

CREATE MATERIALIZED VIEW varcharn_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_array_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT f_c1) AS c1, ARRAY_AGG(DISTINCT f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varchan_array_agg_gby AS SELECT
id, ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_array_agg AS SELECT
ARRAY_AGG(f_c1) AS c1, ARRAY_AGG(f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_array_where AS SELECT
ARRAY_AGG(f_c1) FILTER(WHERE len(f_c2)>4) AS c1, ARRAY_AGG(f_c2) FILTER(WHERE len(f_c2)>4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_array_where_gby AS SELECT
id, ARRAY_AGG(f_c1) FILTER(WHERE len(f_c2)>4) AS c1, ARRAY_AGG(f_c2) FILTER(WHERE len(f_c2)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_count AS SELECT
COUNT(*) AS count
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_count_gby AS SELECT
id, COUNT(*) AS count
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_count_where AS SELECT
COUNT(*) FILTER(WHERE len(f_c1)>4) AS count
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE len(f_c1)>4) AS count
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_count_col AS SELECT
COUNT(f_c1) AS c1, COUNT(f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_count_col_distinct AS SELECT
COUNT(DISTINCT f_c1) AS c1, COUNT(DISTINCT f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT f_c1) AS c1, COUNT(DISTINCT f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_count_col_gby AS SELECT
id, COUNT(f_c1) AS c1, COUNT(f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_count_col_where AS SELECT
COUNT(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, COUNT(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_count_col_where_gby AS SELECT
id, COUNT(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, COUNT(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_min_distinct AS SELECT
MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_min_distinct_gby AS SELECT
id, MIN(DISTINCT f_c1) AS c1, MIN(DISTINCT f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_min_gby AS SELECT
id, MIN(f_c1) AS c1, MIN(f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_min AS SELECT
MIN(f_c1) AS c1, MIN(f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_min_where AS SELECT
MIN(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, MIN(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_min_where_gby AS SELECT
id, MIN(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, MIN(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_max_distinct AS SELECT
MAX(DISTINCT f_c1) AS c1, MAX(DISTINCT f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_max_distinct_gby AS SELECT
id, MAX(DISTINCT f_c1) AS c1, MAX(DISTINCT f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_max_gby AS SELECT
id, MAX(f_c1) AS c1, MAX(f_c2) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_max AS SELECT
MAX(f_c1) AS c1, MAX(f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_max_where AS SELECT
MAX(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, MAX(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_max_where_gby AS SELECT
id, MAX(f_c1) FILTER(WHERE len(f_c1)>4) AS c1, MAX(f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmax_distinct AS SELECT
ARG_MAX(DISTINCT f_c1, f_c2) AS c1, ARG_MAX(DISTINCT f_c2, f_c1) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmax_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT f_c1, f_c2) AS c1, ARG_MAX(DISTINCT f_c2, f_c1) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmax_gby AS SELECT
id, ARG_MAX(f_c1, f_c2) AS c1, ARG_MAX(f_c2, f_c1) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmax AS SELECT
ARG_MAX(f_c1, f_c2) AS c1, ARG_MAX(f_c2, f_c1) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmax_diff AS SELECT
ARG_MAX(f_c1||f_c2, f_c2||f_c1) AS c1, ARG_MAX(f_c2||f_c1, f_c1||f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmax_where AS SELECT
ARG_MAX(f_c1, f_c2) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MAX(f_c2, f_c1) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmax_where_gby AS SELECT
id, ARG_MAX(f_c1, f_c2) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MAX(f_c2, f_c1) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmax_where_gby_diff AS SELECT
id, ARG_MAX(f_c1||f_c2, f_c2||f_c1) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MAX(f_c2||f_c1, f_c1||f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmin_distinct AS SELECT
ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmin_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmin_gby AS SELECT
id, ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmin AS SELECT
ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmin_diff AS SELECT
ARG_MIN(f_c1||f_c2, f_c2||f_c1) AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmin_where AS SELECT
ARG_MIN(f_c1, f_c2) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_argmin_where_gby AS SELECT
id, ARG_MIN(f_c1, f_c2) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_argmin_where_gby_diff AS SELECT
id, ARG_MIN(f_c1||f_c2, f_c2||f_c1) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) FILTER(WHERE len(f_c1)>4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_every AS SELECT
EVERY(f_c1 != '%hello%') AS c1, EVERY(f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_every_distinct AS SELECT
EVERY(DISTINCT f_c1 != '%hello%') AS c1, EVERY(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_every_distinct_gby AS SELECT
id, EVERY(DISTINCT f_c1 != '%hello%') AS c1, EVERY(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_every_gby AS SELECT
id, EVERY(f_c1 != '%hello%') AS c1, EVERY(f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_every_where AS SELECT
EVERY(f_c1 != '%hello%') FILTER(WHERE len(f_c2)=4) AS c1, EVERY(f_c2 LIKE '%a%') FILTER(WHERE len(f_c2)=4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_every_where_gby AS SELECT
id, EVERY(f_c1 != '%hello%') FILTER(WHERE len(f_c2)=4) AS c1, EVERY(f_c2 LIKE '%a%') FILTER(WHERE len(f_c2)=4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_some AS SELECT
SOME(f_c1 != '%hello%') AS c1, SOME(f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_some_distinct AS SELECT
SOME(DISTINCT f_c1 != '%hello%') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_some_distinct_gby AS SELECT
id, SOME(DISTINCT f_c1 != '%hello%') AS c1, SOME(DISTINCT f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_some_gby AS SELECT
id, SOME(f_c1 != '%hello%') AS c1, SOME(f_c2 LIKE '%a%') AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW varcharn_some_where AS SELECT
SOME(f_c1 != '%hello%') FILTER(WHERE len(f_c2)=4) AS c1, SOME(f_c2 LIKE '%a%') FILTER(WHERE len(f_c2)=4) AS c2
FROM atbl_varcharn;

CREATE MATERIALIZED VIEW varcharn_some_where_gby AS SELECT
id, SOME(f_c1 != '%hello%') FILTER(WHERE len(f_c2)=4) AS c1, SOME(f_c2 LIKE '%a%') FILTER(WHERE len(f_c2)=4) AS c2
FROM atbl_varcharn
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_arg_max AS SELECT
ARG_MAX(c1, c2) AS arg_max1,
ARG_MAX(c2, c1) AS arg_max2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_arg_max_gby AS SELECT
id,
ARG_MAX(c1, c2) AS arg_max1,
ARG_MAX(c2, c1) AS arg_max2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_arg_max_where AS SELECT
ARG_MAX(c1, c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max1,
ARG_MAX(c2, c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_arg_max_where_gby AS SELECT
id,
ARG_MAX(c1, c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max1,
ARG_MAX(c2, c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS arg_max2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_arg_min AS SELECT
ARG_MIN(c1, c2) AS arg_min1,
ARG_MIN(c2, c1) AS arg_min2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_arg_min_gby AS SELECT
id,
ARG_MIN(c1, c2) AS arg_min1,
ARG_MIN(c2, c1) AS arg_min2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_arg_min_where AS SELECT
ARG_MIN(c1, c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min1,
ARG_MIN(c2, c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_arg_min_where_gby AS SELECT
id,
ARG_MIN(c1, c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min1,
ARG_MIN(c2, c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS arg_min2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_max AS SELECT
MAX(c1) AS max1,
MAX(c2) AS max2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_max_gby AS SELECT
id,
MAX(c1) AS max1,
MAX(c2) AS max2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_max_where AS SELECT
MAX(c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max1,
MAX(c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_max_where_gby AS SELECT
id,
MAX(c1) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max1,
MAX(c2) FILTER(WHERE c2!= 'cd88e41a-6ffb-4aaf-826e-efffdedf3c31') AS max2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_min AS SELECT
MIN(c1) AS min1,
MIN(c2) AS min2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_min_gby AS SELECT
id,
MIN(c1) AS min1,
MIN(c2) AS min2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_min_where AS SELECT
MIN(c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min1,
MIN(c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min2
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_min_where_gby AS SELECT
id,
MIN(c1) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min1,
MIN(c2) FILTER(WHERE c2!= '859d5430-c3e1-4544-b107-b888582902e2') AS min2
FROM uuid_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW uuid_to_binary AS SELECT
id,
CAST(c1 AS BINARY) AS c1_bin,
CAST(c2 AS BINARY(16)) AS c2_bin
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_to_char AS SELECT
id,
CAST(c1 AS CHAR(8)) AS c1_char,
CAST(c2 AS CHAR) AS c2_char
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_to_varbinary AS SELECT
id,
CAST(c1 AS VARBINARY) AS c1_vbin,
CAST(c2 AS VARBINARY) AS c2_vbin
FROM uuid_tbl;

CREATE MATERIALIZED VIEW uuid_to_vchar AS SELECT
id,
CAST(c1 AS VARCHAR) AS c1_vchar,
CAST(c2 AS VARCHAR) AS c2_vchar
FROM uuid_tbl;

CREATE MATERIALIZED VIEW map_arg_max_distinct AS SELECT
ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_arg_max_distinct_gby AS SELECT
id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_arg_max_gby AS SELECT
id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_arg_max AS SELECT
ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_arg_max_where AS SELECT
ARG_MAX(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_arg_max_where_gby AS SELECT
id, ARG_MAX(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_arg_min_distinct AS SELECT
ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_arg_min_distinct_gby AS SELECT
id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_arg_min_gby AS SELECT
id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_arg_min AS SELECT
ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_arg_min_where AS SELECT
ARG_MIN(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_arg_min_where_gby AS SELECT
id, ARG_MIN(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_array_agg_distinct AS SELECT
ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_array_agg_distinct_gby AS SELECT
id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_array_agg_gby AS SELECT
id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_array_agg AS SELECT
ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_array_where AS SELECT
ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_array_where_gby AS SELECT
id, ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_count AS SELECT
COUNT(*) AS count
FROM map_tbl;

CREATE MATERIALIZED VIEW map_count_gby AS SELECT
id, COUNT(*) AS count
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_count_where AS SELECT
COUNT(*) FILTER(WHERE c1 < c2) AS count
FROM map_tbl;

CREATE MATERIALIZED VIEW map_count_where_gby AS SELECT
id, COUNT(*) FILTER(WHERE c1 < c2) AS count
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_count_col AS SELECT
COUNT(c1) AS c1, COUNT(c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_count_col_distinct AS SELECT
COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_count_col_distinct_gby AS SELECT
id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_count_col_gby AS SELECT
id, COUNT(c1) AS c1, COUNT(c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_count_col_where AS SELECT
COUNT(c1) FILTER(WHERE c1 < c2) AS c1, COUNT(c2) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_count_col_where_gby AS SELECT
id, COUNT(c1) FILTER(WHERE c1 < c2) AS c1, COUNT(c2) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_max_distinct AS SELECT
MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_max_distinct_gby AS SELECT
id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_max_gby AS SELECT
id, MAX(c1) AS c1, MAX(c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_max AS SELECT
MAX(c1) AS c1, MAX(c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_max_where AS SELECT
MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_max_where_gby AS SELECT
id, MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_min_distinct AS SELECT
MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_min_distinct_gby AS SELECT
id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_min_gby AS SELECT
id, MIN(c1) AS c1, MIN(c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_min AS SELECT
MIN(c1) AS c1, MIN(c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_min_where AS SELECT
MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_min_where_gby AS SELECT
id, MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_some AS SELECT
SOME(c1 > c2) AS c1, SOME(c2 > c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_some_distinct AS SELECT
SOME(DISTINCT c1 > c2) AS c1, SOME(DISTINCT c2 > c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_some_distinct_gby AS SELECT
id, SOME(DISTINCT c1 > c2) AS c1, SOME(DISTINCT c2 > c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_some_gby AS SELECT
id, SOME(c1 > c2) AS c1, SOME(c2 > c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_some_where AS SELECT
SOME(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_some_where_gby AS SELECT
id, SOME(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, SOME(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_every AS SELECT
EVERY(c1 > c2) AS c1, EVERY(c2 > c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_every_distinct AS SELECT
EVERY(DISTINCT c1 > c2) AS c1, EVERY(DISTINCT c2 > c1) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_every_distinct_gby AS SELECT
id, EVERY(DISTINCT c1 > c2) AS c1, EVERY(DISTINCT c2 > c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_every_gby AS SELECT
id, EVERY(c1 > c2) AS c1, EVERY(c2 > c1) AS c2
FROM map_tbl
GROUP BY id;

CREATE MATERIALIZED VIEW map_every_where AS SELECT
EVERY(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM map_tbl;

CREATE MATERIALIZED VIEW map_every_where_gby AS SELECT
id, EVERY(c1 > c2) FILTER(WHERE c2 IS NOT NULL) AS c1, EVERY(c2 > c1) FILTER(WHERE c2 IS NOT NULL) AS c2
FROM map_tbl
GROUP BY id;
