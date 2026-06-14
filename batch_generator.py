import random
import csv
import logging ##Permite 
import uuid ##Con esto creamos identificadores unicos
import polars as pl 
import pandas as pd

from faker import Faker ##Genera datos falsos: nombres, correos
from datetime import date, datetime, timedelta

#Configuracion logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s', #
    datefmt='%Y-%m-%d-%H:%M:%S', ##Damos formato de fecha tipo, anio/mes/dia con hr:min:seg
    handlers=[logging.StreamHandler()]
)

#Funcion para crear los datos
def create_data(locale: str) -> Faker: #Nos regresara locaciones/paises de donde nostros queramos, en nuestro caso sera MX
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.") #Nos
    return Faker(locale)

#Funcion para generar un registro
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

    return [
        person_name, user_name, email, personal_number, birth_date, address, phone_numer, mac_address, ip_address, clabe, 
        accessed_at, session_duration, donwload_speed, upload_speed, consumed_traffic
    ]

def write_to_csv(file_path: str, rows: int) -> None:
    fake = create_data("es_MX")

    #Definimos los headers
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address", "phone_numer", "mac_address", 
        "ip_address", "clabe", "accessed_at", "session_duration", "donwload_speed", "upload_speed", "consumed_traffic"
    ]

    with open(file_path, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for _ in range(rows):
            writer.writerow(generate_record(fake))

    #Mensaje log
    logging.info(f"Written {rows} records to the csv file.")

def add_id(file_name) ->None:
    df=pl.read_csv(file_name)
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    df.write_csv(file_name)
    logging.info("added UUID to the dataset.")

def update_datetime(file_name: str, run: str) -> None:
    if run == 'next':
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        df = pl.read_csv(file_name)
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv(file_name)
        logging.info("Updated accessed timestamp")

### Hasta aqui ya generamos nuestros datos, faltaria extraerlos ###
#Es parte de nuestro pipeline, ahorita lo utilizaremos como script, pero futuramente, 
# esta base sera utilizada como un framework

if __name__ == "__main__":

    # Logging starting of the process.

    logging.info(f"Started batch processing for {date.today()}.")

    # Define the output file name with today's date.

    #output_file = f"/work_2/data_2/batch_{date.today()}.csv"

    output_file = f"batch_{date.today()}.csv" #En este caso, sse guardo dentro de nuestra carpeta BigData, si 

    #Aplica cuando tengo el archivo en el directorio BD_DrivenPath\chapter_2\work_2 

    #y un nivel abajo esta data_2

    

    # Define number of records: first run - 10_372; next runs random number.

    if str(date.today()) == "2026-05-27":

        records = random.randint(100_372, 100_372)

        run_type = "first"

    else:

        records = random.randint(0, 1_101)

        run_type = "next"

    

    # Generate and write records to the CSV.

    write_to_csv(f"{output_file}", records)

    # Add UUID to dataset.

    add_id(output_file)

    # Update the timestamp.

    update_datetime(output_file, run_type)

    # Logging ending of the process.

    logging.info(f"Finished batch processing {date.today()}.")

    # Leer el archivo CSV generado
    df = pd.read_csv(output_file)

    # Verificar cuantos datos se generaron
    print(f"Cantidad de datos generados: {len(df)}")
    print(f"Cantidad esperada: {records}")

    if len(df) == records:
        print("Si se genero la cantidad correcta de datos.")
    else:
        print("No coincide la cantidad de datos generados.")

    # Imprimir los primeros 10 datos
    print("\nPrimeros 10 datos:")
    print(df.head(10))
