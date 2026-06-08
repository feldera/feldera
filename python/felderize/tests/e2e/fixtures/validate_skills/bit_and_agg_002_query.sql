-- rule: bit_and_agg
-- spark: bit_and(col) — bitwise AND aggregate over all rows in group
-- feldera: BIT_AND(col)
CREATE OR REPLACE TEMP VIEW network_combined AS SELECT device_id, bit_and(mask_bits) AS final_mask FROM network_mask GROUP BY device_id;
