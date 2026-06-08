INSERT INTO product_catalog VALUES
(101, 'Widget Pro', named_struct('color', 'Red', 'weight_kg', CAST(2.50 AS DECIMAL(8,2)), 'in_stock', true)),
(102, 'Gadget Plus', named_struct('color', 'Blue', 'weight_kg', CAST(1.75 AS DECIMAL(8,2)), 'in_stock', false)),
(103, 'Tool Elite', named_struct('color', 'Black', 'weight_kg', CAST(3.20 AS DECIMAL(8,2)), 'in_stock', true)),
(104, 'Device Max', named_struct('color', 'Silver', 'weight_kg', CAST(0.95 AS DECIMAL(8,2)), 'in_stock', true)),
(105, 'Utility X', named_struct('color', 'Green', 'weight_kg', CAST(4.10 AS DECIMAL(8,2)), 'in_stock', false));
