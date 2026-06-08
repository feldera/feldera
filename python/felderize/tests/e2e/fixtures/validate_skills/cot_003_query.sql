-- rule: cot
-- spark: cot(x) — cotangent (1/tan(x))
-- feldera: COT(x) — supported directly in Feldera
CREATE OR REPLACE TEMP VIEW cot_results_v3 AS SELECT sample_id, frequency, phase, cot(phase) AS phase_cotangent, cot(frequency) AS frequency_cotangent FROM signal_analysis;
