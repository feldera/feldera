INSERT INTO employee_info VALUES
(1, 'Alice Johnson', named_struct('department', 'Engineering', 'salary', CAST(85000.00 AS DECIMAL(10,2)))),
(2, 'Bob Smith', named_struct('department', 'Sales', 'salary', CAST(65000.00 AS DECIMAL(10,2)))),
(3, 'Carol White', named_struct('department', 'Engineering', 'salary', CAST(95000.00 AS DECIMAL(10,2)))),
(4, 'David Brown', named_struct('department', 'Marketing', 'salary', CAST(45000.00 AS DECIMAL(10,2)))),
(5, 'Eve Davis', named_struct('department', 'Finance', 'salary', CAST(75000.00 AS DECIMAL(10,2))));
