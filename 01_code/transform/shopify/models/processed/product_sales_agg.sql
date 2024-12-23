{{ 
    config(
        materialized='table'
    ) 
}}

WITH all_lines AS (

SELECT 
  od.order_number
  ,line.product_id AS line_product_id
  ,line.variant_id AS line_variant_id
  ,CAST(variant.price AS NUMERIC) AS price
  ,CAST(line.quantity AS NUMERIC) AS quantity
  ,IFNULL(CAST(discount.amount AS NUMERIC), 0) AS discount
  ,od.total_discounts
  ,od.financial_status AS payment_status
FROM
   {{ ref('order') }} AS od
LEFT JOIN
  UNNEST(od.line_items) AS line
LEFT JOIN
   {{ ref('product') }} AS pd
  ON line.product_id = pd.id
LEFT JOIN
  UNNEST(pd.variants) AS variant
  ON line.variant_id = variant.id
LEFT JOIN
  UNNEST(line.discount_allocations) AS discount
WHERE od.financial_status IN ('paid') -- only counting sales from paid orders
)

-- same product within an order can have different discounts applied. Need to group down to variant level and sum discounts
,discount_summed AS(
  SELECT
    order_number
    ,line_product_id
    ,line_variant_id
    ,quantity
    ,SUM(discount) AS discount
  FROM all_lines
  GROUP BY order_number, line_product_id, line_variant_id, quantity
)

-- add the summed discount back to respective row in all_lines via a join
,subtotals AS(
SELECT DISTINCT
  d.order_number
  ,d.line_product_id
  ,d.line_variant_id
  ,l.price
  ,d.quantity
  ,d.discount
  ,(price*d.quantity) - d.discount AS subtotal
FROM discount_summed AS d
LEFT JOIN all_lines AS l
  ON 
    l.order_number = d.order_number AND
    l.line_product_id = d.line_product_id AND 
    l.line_variant_id = d.line_variant_id
)

,agg AS (

SELECT
  line_product_id AS product_id
  ,SUM(quantity) AS product_quantity
  ,SUM(subtotal) AS product_rev
FROM subtotals
GROUP BY line_product_id
)

SELECT * FROM agg
-- ORDER BY order_number

