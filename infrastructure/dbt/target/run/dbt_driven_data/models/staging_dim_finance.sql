
  
    

  create  table "airflow"."driven_raw_staging"."dim_finance__dbt_tmp"
  
  
    as
  
  (
    -- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date


WITH source_data AS (
    SELECT
        unique_id,
        clabe
    FROM 
        "airflow"."driven_raw"."raw_batch_data"
)
        SELECT *
        FROM source_data
  );
  