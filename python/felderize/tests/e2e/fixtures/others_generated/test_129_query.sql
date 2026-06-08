CREATE OR REPLACE TEMP VIEW bm550_creative_posexplode AS
SELECT
  basket_id,
  pos,
  sku_token
FROM creative_baskets
LATERAL VIEW posexplode(skus) pe AS pos, sku_token;
