from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_world",
    schedule_interval=None,   # only run manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow!'"
    )
