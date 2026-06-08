-- rule: current_date
-- spark: CURRENT_DATE — today's date
-- feldera: CAST(NOW() AS DATE)
CREATE TABLE user_registrations (
  user_id INT,
  username STRING,
  registration_date DATE,
  status STRING
);
