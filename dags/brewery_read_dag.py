from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


with DAG(
    dag_id='brewery_read_dag',
    default_args={
        'retries': 0
    },
    start_date=datetime(2025, 2, 10),
    template_searchpath='/usr/local/airflow/include',
    catchup=False
) as breweries_read_dag:


    read_notebook_task = PapermillOperator(
        task_id="bronze_brewery",
        input_nb="include/notebooks/brewery_read_gold.ipynb",
        output_nb="include/notebooks/runs/read-{{ ts_nodash }}.ipynb",
    )

    read_notebook_task 