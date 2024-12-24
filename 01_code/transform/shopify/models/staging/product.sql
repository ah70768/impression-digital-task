{{ 
    config(
        materialized='table'
    ) 
}}

WITH stage_product AS
(
SELECT 
  id
  ,title
  ,body_html
  ,vendor
  ,product_type
  ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', created_at) AS created_at
  ,handle
  ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', updated_at) AS updated_at
  ,PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', published_at) AS published_at
  ,template_suffix
  ,published_scope
  ,tags
  ,status
  ,admin_graphql_api_id
  ,variants
  ,options
  ,images
  ,image
FROM raw.product
)

SELECT * FROM stage_product