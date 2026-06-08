INSERT INTO inventory_snapshot (warehouse_id, sku, on_hand, flagged) VALUES (100, 'alpha', 1, true);
INSERT INTO inventory_snapshot (warehouse_id, sku, on_hand, flagged) VALUES (200, 'beta', 2, false);
INSERT INTO inventory_snapshot (warehouse_id, sku, on_hand, flagged) VALUES (300, 'gamma', 3, true);
INSERT INTO inventory_snapshot (warehouse_id, sku, on_hand, flagged) VALUES (400, 'delta', 4, false);
INSERT INTO inventory_snapshot (warehouse_id, sku, on_hand, flagged) VALUES (500, 'epsilon', 5, true);

INSERT INTO inventory_flags (warehouse_id, sku, flagged, review_status) VALUES (100, 'alpha', true, 'alpha');
INSERT INTO inventory_flags (warehouse_id, sku, flagged, review_status) VALUES (200, 'beta', false, 'beta');
INSERT INTO inventory_flags (warehouse_id, sku, flagged, review_status) VALUES (300, 'gamma', true, 'gamma');
INSERT INTO inventory_flags (warehouse_id, sku, flagged, review_status) VALUES (400, 'delta', false, 'delta');
INSERT INTO inventory_flags (warehouse_id, sku, flagged, review_status) VALUES (500, 'epsilon', true, 'epsilon');
