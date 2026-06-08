-- rule: float_type
-- spark: FLOAT type in CREATE TABLE DDL
-- feldera: REAL — Feldera uses REAL instead of FLOAT
CREATE TABLE energy_consumption (timestamp TIMESTAMP, location STRING, watts_used FLOAT, efficiency_ratio FLOAT);
