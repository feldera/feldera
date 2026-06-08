-- rule: sec
-- spark: sec(x) — secant (1/cos(x))
-- feldera: SEC(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW sec_results_v3 AS SELECT wave_id, angle_value, amplitude, sec(angle_value) * amplitude AS adjusted_secant FROM waveform_angles ORDER BY wave_id;
