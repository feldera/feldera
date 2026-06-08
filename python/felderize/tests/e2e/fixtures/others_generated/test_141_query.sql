CREATE OR REPLACE TEMP VIEW gpt141_log_base AS
SELECT
  sample_id,
  channel,
  amplitude,
  LOG(amplitude, 2) AS log2_amplitude,
  LOG(amplitude, 10) AS log10_amplitude
FROM audio_levels
WHERE amplitude > 0;
