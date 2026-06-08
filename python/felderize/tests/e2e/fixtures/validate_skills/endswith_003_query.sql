-- rule: endswith
-- spark: endswith(s, suffix) — true if string ends with suffix
-- feldera: RIGHT(s, LENGTH(suffix)) = suffix
CREATE OR REPLACE TEMP VIEW matching_urls_v3 AS SELECT id, url, suffix FROM url_endpoints WHERE endswith(url, suffix);
