CREATE TABLE
silver_layer.dim_address AS
SELECT
unique_id,
address,
mac_address,
ip_address
FROM
bronze_layer.batch_first_load
---

CREATE TABLE
silver_layer.dim_date AS
SELECT
unique_id,
accessed_at
FROM
bronze_layer.batch_first_load
---

CREATE TABLE
silver_layer.dim_finance AS
SELECT
unique_id,
clabe
FROM
bronze_layer.batch_first_load
---

CREATE TABLE
silver_layer.dim_person AS
SELECT
unique_id,
person_name,
user_name,
email,
phone,
birth_date,
personal_number
FROM
bronze_layer.batch_first_load

--- Creamos nuestra ultima tabla ---
CREATE TABLE
silver_layer.fact_network_usages AS
SELECT
unique_id,
session_duration,
download_speed,
upload_speed,
consumed_traffic
FROM
bronze_layer.batch_first_load4

SELECT * FROM silver_layer.fact_network_usages
