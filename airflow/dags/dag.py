from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from transform_data import transform
from get_data import get

with DAG("desafio_pagseguro", start_date=days_ago(2), schedule_interval=None, catchup=False) as dag:
    
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get
    )   

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )   

    get_data >> transform_data