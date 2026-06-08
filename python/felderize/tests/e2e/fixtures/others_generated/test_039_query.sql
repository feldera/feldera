CREATE OR REPLACE TEMP VIEW bm54_feature_flag_filter AS
SELECT feature_name FROM feature_flags WHERE enabled = true AND (internal_only = false OR internal_only IS NULL);
