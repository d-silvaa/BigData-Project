
  
    

  create  table "airflow"."driven_raw_staging"."dim_person__dbt_tmp"
  
  
    as
  
  (
    -- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date


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
        "airflow"."driven_raw"."raw_batch_data"
)
        SELECT *
        FROM source_data
  );
  