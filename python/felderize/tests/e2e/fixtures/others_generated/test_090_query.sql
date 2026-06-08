CREATE OR REPLACE TEMP VIEW jn06_supply_right_join_chain AS
SELECT
  w.region,
  sup.tier,
  COALESCE(SUM(sh.shipped_qty), 0) AS shipped_qty
FROM supply_inventory i
RIGHT JOIN supply_shipments sh
  ON i.warehouse_id = sh.warehouse_id
 AND i.sku = sh.sku
JOIN supply_suppliers sup
  ON sh.supplier_id = sup.supplier_id
JOIN supply_warehouses w
  ON sh.warehouse_id = w.warehouse_id
GROUP BY w.region, sup.tier;
