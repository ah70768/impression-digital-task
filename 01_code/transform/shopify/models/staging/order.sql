
{{ 
    config(
        materialized='table'
    ) 
}}


WITH correct_prices AS 
(
    SELECT
      od.order_number
      ,line.product_id
      -- order 1010 was paid in MXN but the GBP equivalent price is not in the product or variant table
      ,CASE WHEN od.order_number = 1010 THEN CAST(line.price AS NUMERIC) ELSE CAST(variant.price AS NUMERIC) END AS price
      ,CAST(line.quantity AS NUMERIC) AS quantity
      ,(CASE WHEN od.order_number = 1010 THEN CAST(line.price AS NUMERIC) ELSE CAST(variant.price AS NUMERIC) END) * CAST(line.quantity AS NUMERIC) AS subtotal
    FROM raw.order AS od
    LEFT JOIN UNNEST(od.line_items) AS line
    LEFT JOIN {{ ref('product') }} AS pd
        ON line.product_id = pd.id
    LEFT JOIN UNNEST(pd.variants) AS variant
        ON line.variant_id = variant.id
)

,shipping_price AS
(
    SELECT 
        order_number
        ,CAST(ship.price AS NUMERIC) AS price
    FROM raw.order,
    UNNEST(shipping_lines) AS ship 
)

,subtotals AS
(
SELECT 
  order_number
  ,SUM(subtotal) AS subtotal
FROM correct_prices
GROUP BY order_number
)

,stage_order AS
(
SELECT 
  o.order_number
  ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', created_at) AS created_at
  ,phone
  ,contact_email
  ,po_number
  ,COALESCE(s.subtotal, CAST(o.current_subtotal_price AS NUMERIC)) AS subtotal
  ,CAST(total_discounts AS NUMERIC) AS total_discounts
  ,COALESCE(s.subtotal, CAST(o.current_subtotal_price AS NUMERIC)) - CAST(o.total_discounts AS NUMERIC) AS current_subtotal_price
  ,IFNULL(sp.price,0) AS shipping_cost
  ,COALESCE(s.subtotal, CAST(o.current_subtotal_price AS NUMERIC)) - CAST(o.total_discounts AS NUMERIC) + IFNULL(sp.price,0) AS total_price 
  -- ,CAST(o.current_subtotal_price AS NUMERIC) AS shopify_subtotal 
  -- ,o.total_price AS shopify_subtotal
  ,CAST(total_tax AS NUMERIC) AS total_tax
  ,financial_status
  ,fulfillment_status
  ,note
  ,customer
  ,line_items
  ,refunds
  ,shipping_address
FROM
  raw.order AS o
LEFT JOIN shipping_price AS sp
  ON o.order_number = sp.order_number
LEFT JOIN subtotals AS s
  ON s.order_number = o.order_number


-- ORDER BY o.order_number
)


SELECT *
FROM stage_order

