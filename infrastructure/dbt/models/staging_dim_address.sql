-- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_address 
{{ config(
    materialized='table', 
    schema='staging',
    alias='dim_address', 
    tags=['staging'] 
) }}

WITH source_data AS (
        SELECT
            unique_id,
            address,
            mac_address,
            ip_address
        FROM
            {{source('raw_source', 'raw_batch_data')}}
    
)
        SELECT *
        FROM source_data