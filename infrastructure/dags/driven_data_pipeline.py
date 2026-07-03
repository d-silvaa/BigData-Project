import random
import csv
import logging ##Permite 
import uuid ##Con esto creamos identificadores unicos
import polars as pl 
import pandas as pd

from faker import Faker ##Genera datos falsos: nombres, correos
from datetime import date, datetime, timedelta
##Traemos los operadores de Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
##Traemos los operadores de SQL
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


#Configuracion logs ##Queda igual
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s', #
    datefmt='%Y-%m-%d-%H:%M:%S', ##Damos formato de fecha tipo, anio/mes/dia con hr:min:seg
    handlers=[logging.StreamHandler()]
)

#Funcion para crear los datos ##Queda igual
def create_data(locale: str) -> Faker: #Nos regresara locaciones/paises de donde nostros queramos, en nuestro caso sera MX
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.") #Nos
    return Faker(locale)

#Funcion para generar un registro ##Queda igual
def generate_record(fake: Faker)-> list:
    #Aqui se generan los datos random
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower() #reemplazamos los espacios
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn() #Social Security number. Para nuestro caso sera el numero del seguro social
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ") ##Reemplaze el salto de linea "\n" por ", "
    phone_numer = fake.phone_number()
    mac_address = fake.mac_address()
    ip_address = fake.ipv4() #ip version 4 fake
    clabe = fake.iban() ##El banco, para nosotros es la clabe
    accessed_at = fake.date_time_between("-1y") ##Contiene datos del anio anterior
    session_duration = random.randint(0, 36_000)  ##Maximo 10 hrs (equivalente 36_000)
    donwload_speed = random.randint(0, 1_000) 
    upload_speed = random.randint(0,800)
    consumed_traffic = random.randint(0, 2_000_000)

    ##Queda igual
    return [ 
        person_name, user_name, email, personal_number, birth_date, address, phone_numer, mac_address, ip_address, clabe, 
        accessed_at, session_duration, donwload_speed, upload_speed, consumed_traffic
    ]

#Eliminamos los parametros (lo que esta entre parentesis)
def write_to_csv() -> None:
    fake = create_data("es_MX")

    #Definimos los headers ##Queda igual
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address", "phone_numer", "mac_address", 
        "ip_address", "clabe", "accessed_at", "session_duration", "donwload_speed", "upload_speed", "consumed_traffic"
    ]
    #Agregamos un if para las filas basandonos en la fecha
    if str(date.today()) == "2026-06-09":
        rows = random.randint(100_372, 100_372)
    else:
        rows = random.randint(0, 1_101)

    ##Agregamos la ruta de airflow en en lugar del file_path de nuestra computadora local
    with open("/opt/airflow/data/raw_data.csv", mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for _ in range(rows):
            writer.writerow(generate_record(fake))

    #Mensaje log ##Queda igual
    logging.info(f"Written {rows} records to the csv file.")

##Elinamos los parametros
##Cambiamos la ruta por nuestra ruta airflow
def add_id() ->None:
    df=pl.read_csv("/opt/airflow/data/raw_data.csv")
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    df.write_csv("/opt/airflow/data/raw_data.csv")
    logging.info("added UUID to the dataset.")

##Eliminamos los parametros
##Cambiamos la ruta por nuestra ruta airflow
def update_datetime() -> None:
    current_time = datetime.now().replace(microsecond=0)
    yesterday_time = str(current_time - timedelta(days=1))
    df = pl.read_csv("/opt/airflow/data/raw_data.csv")
    df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
    df.write_csv("/opt/airflow/data/raw_data.csv")
    logging.info("Updated accessed timestamp")

### Hasta aqui ya generamos nuestros datos, faltaria extraerlos ###
#Es parte de nuestro pipeline, ahorita lo utilizaremos como script, pero futuramente, 
#esta base sera utilizada como un framework

##Agregamos una nueva funcion y borramos el if
def save_raw_data():
    # Logging starting of the process.

    logging.info(f"Started batch processing for {date.today()}.")

    # Define the output file name with today's date.
    #output_file = f"/work_2/data_2/batch_{date.today()}.csv"
    ##Agregamos
    write_to_csv()
    add_id()
    update_datetime()
    logging.info(f"finish_batch_proccesed {date.today()}")

## Configuracion general del DAG.
# Aqui se definen parametros que seran compartidos por todas las tareas.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0
}

## Definición del DAG principal.
# Este DAG sera responsable de ejecutar el pipeline completo
# desde la extraccion de datos hasta la ejecucion de modelos DBT.
dag = DAG(
    dag_id = 'extract_raw_data_pipeline',
    default_args = default_args,
    description = 'DataDriven Main Pipeline.',
    schedule = "* 7 * * *",
    start_date = datetime(2024, 9, 22),
    catchup = False,
)

## Definimos la tarea encargada de generar y almacenar los datos sinteticos 
# que posteriormente serán cargados a PostgreSQL.
extract_raw_data_task = PythonOperator(
    task_id = 'extract_raw_data',
    python_callable = save_raw_data,
    dag = dag,
)

## Crea el esquema driven_raw en PostgreSQL.
### OJO, este esquema representa la capa Bronze del proyecto ###
create_raw_schema_task = SQLExecuteQueryOperator(
    task_id = 'create_raw_schema',
    conn_id = 'postgres_conn',
    sql = 'CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag = dag,
)

## Crea la tabla raw_batch_data donde se almacenaran
# los datos sin transformar provenientes del CSV.
create_raw_table_task = SQLExecuteQueryOperator(
    task_id = 'create_raw_table',
    conn_id = 'postgres_conn',
    sql = """
         CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data(
            person_name VARCHAR(100),
            user_name VARCHAR(100),
            email VARCHAR(100),
            personal_number NUMERIC,
            birth_date VARCHAR(100),
            address VARCHAR(250),
            phone VARCHAR(100),
            mac_address VARCHAR(100),
            ip_address VARCHAR(100),
            clabe VARCHAR(100),
            accessed_at TIMESTAMP,
            session_duration INT,
            download_speed INT,
            upload_speed INT,
            consumed_traffic INT,
            unique_id VARCHAR(100)
        );
    """,
    dag = dag
)

## Carga el CSV generado previamente dentro de la tabla raw_batch_data utilizando COPY.
load_raw_data_task = SQLExecuteQueryOperator(
    task_id = 'load_raw_data',
    conn_id = 'postgres_conn',
    sql = """
    COPY driven_raw.raw_batch_data(
    person_name, user_name, email, personal_number,
    birth_date, address, phone, mac_address, ip_address,
    clabe, accessed_at, session_duration, download_speed,
    upload_speed, consumed_traffic, unique_id
    )
    FROM '/opt/airflow/data/raw_data.csv'
    DELIMITER ','
    CSV HEADER;
    """
)

## Run dbt
## Ejecuta los modelos DBT etiquetados como 'staging' de la capa Silver.
run_dbt_staging_task = BashOperator(
    task_id = 'run_dbt_staging',
    bash_command = 'set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
)

## Ejecuta los modelos DBT de la capa Trusted (Gold).
# Los datos quedan listos para analisis y consumo.
run_dbt_trusted_task = BashOperator(
    task_id = 'run_dbt_trusted',
    bash_command = 'set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
)

#Dependencias
## Definicion del flujo de dependencias.
# Establece el orden de ejecución de las tareas dentro del DAG.
[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task