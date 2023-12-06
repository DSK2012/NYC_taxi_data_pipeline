from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import sys
from datetime import datetime

sys.path.append('/opt/airflow/myscripts')
from src import transformation
from data_scraper import scraper

with DAG(
        dag_id = "nyc_pipeline",
        start_date = datetime(2023,10,19),
        schedule = "@monthly",
        catchup = False
        ) as dag:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=scraper
    )

    transform_load_data = PythonOperator(
        task_id="transform_load_data",
        python_callable=transformation
    )



    extract_data >> transform_load_data