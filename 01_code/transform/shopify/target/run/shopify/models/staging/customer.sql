
  
    

    create or replace table `impression-digital`.`staging`.`customer`
      
    
    

    OPTIONS()
    as (
      

WITH raw_cust AS (

SELECT
  id
  ,CONCAT(first_name, ' ', last_name) AS customer_name
  ,email
  ,phone
FROM impression-digital.raw.Customer

)

SELECT *
FROM raw_cust
    );
  