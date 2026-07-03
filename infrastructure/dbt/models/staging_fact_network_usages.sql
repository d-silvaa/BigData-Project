-- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date
{{ config(
    materialized='table', 
    schema='staging',
    alias='fact_network_usages', 
    tags=['staging'] 
) }}

WITH source_data AS (
    SELECT
        unique_id,
        session_duration,
        download_speed,
        upload_speed,
        consumed_traffic
    FROM 
        {{source('raw_source', 'raw_batch_data')}}
)
        SELECT *
        FROM source_data