
-- tests the corrected total_price in the staging.order table against user input value below

WITH expected_values AS (
    SELECT 
        1001 AS order_number, 3377.88 AS expected_value UNION ALL
        SELECT 1002, 723.94 UNION ALL
        SELECT 1003, 753.94 UNION ALL
        SELECT 1004, 1800.37 UNION ALL
        SELECT 1005, 2123.84 UNION ALL
        SELECT 1006, 1283.90 UNION ALL
        SELECT 1007, 223.99 UNION ALL
        SELECT 1008, 885.95 UNION ALL
        SELECT 1009, 885.95 UNION ALL
        SELECT 1010, 2723.28
)


SELECT 
  o1.order_number
  ,o1.expected_value
  ,o2.total_price
FROM expected_values o1
LEFT JOIN {{ ref('order') }} AS o2
  ON o1.order_number = o2.order_number
WHERE o1.expected_value != o2.total_price
-- ORDER BY order_number