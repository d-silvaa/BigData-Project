#Use the official Airflow image as template
FROM apache/airflow:2.10.2
#switch to airflow user to install packages.
USER airflow
#copy the requierements file into the container.
COPY requirements.txt  .
#install all dependencies from the rquierements file.
RUN pip install -r requirements.txt
#Xopy dbt directory into the container.
COPY dbt /opt/airflow/dbt