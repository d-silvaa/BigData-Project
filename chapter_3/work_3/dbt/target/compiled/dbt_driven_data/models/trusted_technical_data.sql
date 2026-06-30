-- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date


-- Como le pusimos alias a nuestras tablas, ahora le tenemos que poner los alias que le correesponden
-- para min_session_... tambien hicimos un calculo, por lo que se lo pasaremos
WITH source_data AS (
    SELECT
        fnu.unique_id,
        da.address,
        da.mac_address,
        da.ip_address,
        fnu.download_speed,
        fnu.upload_speed,
        round(fnu.session_duration/60, 1) as min_session_duration,
        case when download_speed < 50 or upload_speed< 30 or (fnu.session_duration/60) < 1 then true 
        else false end as technical_issue
    FROM 
        "airflow"."driven_raw_staging"."fact_network_usages" fnu
    JOIN
        "airflow"."driven_raw_staging"."dim_address" da
        ON fnu.unique_id = da.unique_id 
)
        SELECT *
        FROM source_data