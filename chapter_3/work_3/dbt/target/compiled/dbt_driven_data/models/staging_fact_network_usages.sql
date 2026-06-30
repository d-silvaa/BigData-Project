-- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date


WITH source_data AS (
    SELECT
        unique_id,
        session_duration,
        download_speed,
        upload_speed,
        consumed_traffic
    FROM 
        "airflow"."driven_raw"."raw_batch_data"
)
        SELECT *
        FROM source_data