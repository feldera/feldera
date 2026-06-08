-- rule: isnan
-- spark: isnan(x) — true if x is IEEE 754 NaN
-- feldera: IS_NAN(x)
CREATE OR REPLACE TEMP VIEW data_quality_check_v3 AS SELECT experiment_id, NOT isnan(measurement_x) AS x_valid, NOT isnan(measurement_y) AS y_valid, NOT isnan(measurement_z) AS z_valid FROM scientific_data;
