
-- tests that product_id column is unique in product_sales_agg model

WITH unique_count AS (
    SELECT
        product_id,
        COUNT(*) AS count
    FROM {{ ref('product_sales_agg') }}
    GROUP BY product_id
    HAVING COUNT(*) > 1
)
SELECT *
FROM unique_count