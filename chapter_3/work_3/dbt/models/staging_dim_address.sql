#Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
#nuestra capa staging y se llamara dim_address 
{{ config(
    materialized='table', ##Creamos fisicamente la tabla
    schema='staying',
    alias='dim_address',
    tags=['staging']
) }}

WITH source_data AS (
    
)