-- rule: endswith
-- spark: endswith(s, suffix) — true if string ends with suffix
-- feldera: RIGHT(s, LENGTH(suffix)) = suffix
CREATE TABLE url_endpoints (id INT, url STRING, suffix STRING);
