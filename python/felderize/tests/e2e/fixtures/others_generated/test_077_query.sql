CREATE OR REPLACE TEMP VIEW val62_natural_inventory AS
SELECT warehouse_id, sku, on_hand, review_status
FROM inventory_snapshot
NATURAL JOIN inventory_flags;
