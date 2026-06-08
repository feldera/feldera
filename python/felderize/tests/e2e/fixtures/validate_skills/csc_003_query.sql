-- rule: csc
-- spark: csc(x) — cosecant (1/sin(x))
-- feldera: CSC(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW csc_results_v3 AS SELECT signal_id, phase, amplitude, ROUND(csc(phase), 6) AS csc_phase, amplitude * csc(phase) AS scaled_csc FROM wave_analysis WHERE phase > 0.1 AND phase < 3.0;
