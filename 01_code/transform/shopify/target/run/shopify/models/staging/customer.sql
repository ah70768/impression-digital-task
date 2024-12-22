
  
    

    create or replace table `impression-digital`.`staging`.`customer`
      
    
    

    OPTIONS()
    as (
      

WITH raw_cust AS (

SELECT
  id
  ,email
  ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', created_at) AS created_at
  ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', updated_at) AS updated_at
  ,first_name
  ,last_name
  ,CAST(orders_count AS INT64) AS orders_count
  ,state
  ,CAST(total_spent AS FLOAT64) AS total_spent
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
  impression-digital.raw.Customer
)

SELECT *
FROM raw_cust
    );
  