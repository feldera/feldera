-- rule: log2
-- spark: log2(x) — base-2 logarithm
-- feldera: LOG(x, 2)
CREATE OR REPLACE TEMP VIEW signal_analysis_v1 AS SELECT signal_id, power_level, log2(power_level) AS log2_power FROM signal_power WHERE power_level > 0;
