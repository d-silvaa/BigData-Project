-- Su funcion es leer los datos desde nuestra tabla fuente y va a crear una tabla en 
-- nuestra capa staging y se llamara dim_date


-- Como le pusimos alias a nuestras tablas, ahora le tenemos que poner los alias que le correesponden
WITH source_data AS (
    SELECT
        dp.person_name,
        dp.user_name,
        dp.email,
        dp.personal_number,
        dp.birth_date,
        da.address,
        dp.phone,
        da.mac_address,
        da.ip_address,
        df.clabe,
        dd.accessed_at,
        fnu.session_duration,
        fnu.upload_speed,
        fnu.consumed_traffic,
        fnu.unique_id
    FROM 
        "airflow"."driven_raw_staging"."fact_network_usages" fnu
    INNER JOIN
        "airflow"."driven_raw_staging"."dim_address" da
        ON fnu.unique_id = da.unique_id
    INNER JOIN
        "airflow"."driven_raw_staging"."dim_date" dd
        ON da.unique_id = dd.unique_id
    INNER JOIN
        "airflow"."driven_raw_staging"."dim_finance" df
        ON dd.unique_id = df.unique_id
    INNER JOIN
        "airflow"."driven_raw_staging"."dim_person" dp
        ON df.unique_id = dp.unique_id
)
        SELECT *
        FROM source_data