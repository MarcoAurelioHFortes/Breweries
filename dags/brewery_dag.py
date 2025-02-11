from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


with DAG(
    dag_id='brewery_dag',
    default_args={
        'retries': 4
    },
    schedule='0 0 * * *',
    start_date=datetime(2025, 2, 10),
    template_searchpath='/usr/local/airflow/include',
    catchup=False
) as breweries_dag:


    bronze_notebook_task = PapermillOperator(
        task_id="bronze_brewery",
        input_nb="include/notebooks/brewery_bronze.ipynb",
        output_nb="include/notebooks/runs/bronze-{{ ts_nodash }}.ipynb",
        parameters={"raw_dir": "{{ ts_nodash  }}"},
    )

    silver_notebook_task = PapermillOperator(
        task_id="silver_brewery",
        input_nb="include/notebooks/brewery_silver.ipynb",
        output_nb="include/notebooks/runs/silver-{{ ts_nodash  }}.ipynb",
        parameters={"raw_dir": "{{ ts_nodash }}"},
    )

    gold_notebook_task = PapermillOperator(
        task_id="gold_brewery",
        input_nb="include/notebooks/brewery_gold.ipynb",
        output_nb="include/notebooks/runs/gold-{{ ts_nodash  }}.ipynb",
        parameters={"raw_dir": "{{ ts_nodash }}"},
    )

    bronze_notebook_task >>  silver_notebook_task >> gold_notebook_task