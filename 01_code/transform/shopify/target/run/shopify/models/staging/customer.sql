
  
    

    create or replace table `impression-digital`.`staging`.`customer`
      
    
    

    OPTIONS()
    as (
      

WITH cust_spend AS
(
  SELECT
    customer.id
    ,SUM(current_subtotal_price) AS subtotal_spent
    ,SUM(total_price) AS total_spent
  FROM `impression-digital`.`staging`.`order`
  GROUP BY customer.id
)

,stage_cust AS
(
  SELECT
    c.id
    ,email
    ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', created_at) AS created_at
    ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', updated_at) AS updated_at
    ,first_name
    ,last_name
    ,CAST(orders_count AS INT64) AS orders_count
    ,state
    -- ,s.total_spent
    ,COALESCE(s.total_spent, CAST(c.total_spent AS FLOAT64), 0) AS total_spent
    ,CAST(last_order_id AS STRING) AS last_order_id
    ,note
    ,CAST(verified_email AS BOOL) AS verified_email
    ,multipass_identifier
    ,CAST(tax_exempt AS BOOL) AS tax_exempt
    ,tags
    ,last_order_name
    ,currency
    ,phone
    ,addresses
    ,tax_exemptions
    ,email_marketing_consent
    ,sms_marketing_consent
    ,admin_graphql_api_id
    ,default_address
  FROM
    raw.customer AS c
  LEFT JOIN cust_spend AS s
  ON c.id = s.id
)

SELECT * FROM stage_cust
    );
  