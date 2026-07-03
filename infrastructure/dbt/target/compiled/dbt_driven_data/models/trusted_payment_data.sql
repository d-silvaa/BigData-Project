-- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date


-- Como le pusimos alias a nuestras tablas, ahora le tenemos que poner los alias que le correesponden
-- como para payment_data hicimos un calculo, lo que haremos es que le pasaremos el calculo
WITH source_data AS (
    SELECT
        fnu.unique_id,
        df.clabe,
        fnu.download_speed,
        fnu.upload_speed, 
        fnu.session_duration,
        fnu.consumed_traffic,
        ((fnu.download_speed + fnu.upload_speed + 1) / 2 + (fnu.consumed_traffic / fnu.session_duration)) as payment_amount
    FROM 
        "airflow"."driven_raw_staging"."fact_network_usages" fnu
    JOIN
        "airflow"."driven_raw_staging"."dim_finance" df
        ON fnu.unique_id = df.unique_id 
)
        SELECT *
        FROM source_data