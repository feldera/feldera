-- rule: array_except
-- spark: array_except(a, b) — elements in a not in b
-- feldera: ARRAY_EXCEPT(a, b)
CREATE TABLE restaurant_menu_v3 (menu_id INT, available_items ARRAY<STRING>, out_of_stock ARRAY<STRING>);
