-- rule: isnan
-- spark: isnan(x) — true if x is IEEE 754 NaN
-- feldera: IS_NAN(x)
CREATE TABLE scientific_data (experiment_id INT, measurement_x DOUBLE, measurement_y DOUBLE, measurement_z DOUBLE); CREATE TABLE data_quality (experiment_id INT, x_valid BOOLEAN, y_valid BOOLEAN, z_valid BOOLEAN);
