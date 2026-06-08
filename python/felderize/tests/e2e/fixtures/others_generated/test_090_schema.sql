CREATE TABLE supply_warehouses (
  warehouse_id BIGINT,
  region STRING,
  capacity INT
) USING parquet;

CREATE TABLE supply_inventory (
  warehouse_id BIGINT,
  sku STRING,
  supplier_id BIGINT,
  on_hand INT
) USING parquet;

CREATE TABLE supply_shipments (
  shipment_id BIGINT,
  warehouse_id BIGINT,
  sku STRING,
  supplier_id BIGINT,
  shipped_qty INT
) USING parquet;

CREATE TABLE supply_suppliers (
  supplier_id BIGINT,
  supplier_name STRING,
  tier STRING
) USING parquet;

CREATE TABLE supply_restocks (
  restock_id BIGINT,
  warehouse_id BIGINT,
  supplier_id BIGINT,
  planned_qty INT
) USING parquet;
