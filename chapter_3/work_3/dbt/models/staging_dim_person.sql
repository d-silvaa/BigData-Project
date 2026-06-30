-- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date
{{ config(
    materialized='table', 
    schema='staging',
    alias='dim_person', 
    tags=['staging'] 
) }}

WITH source_data AS (
    SELECT
        unique_id,
        person_name,
        user_name,
        email,
        phone,
        birth_date,
        personal_number
    FROM 
        {{source('raw_source', 'raw_batch_data')}}
)
        SELECT *
        FROM source_data