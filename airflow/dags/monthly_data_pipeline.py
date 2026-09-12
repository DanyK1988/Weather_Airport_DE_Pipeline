from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta


# Мы используем повторную попытку только в load_api, если бы у нас было несколько task с похожим запросом
# то логичнее использовать одинаковые значения для всех одинаковое
'''
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": ["your_email@example.com],
    "email_on_failure": True,
    "email_on_retry": False,}
'''

with DAG(
    dag_id="monthly_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 1 * *",
    catchup=False,
    tags=["flight_project"],
    #default_args=default_args,
) as dag:

    load_files = BashOperator(
        task_id="load_files",
        bash_command="cd /app && python main.py files",
    )

    load_api = BashOperator(
        task_id="load_api",
        bash_command=(
            "cd /app && "
         "START_DATE={{ macros.ds_add(ds, -1 * var.value.load_lookback_days|int) }} "
         "END_DATE={{ ds }} "
         "python main.py api"
        ),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /app/flight_dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /app/flight_dbt && dbt test",
    )

    load_files >> load_api >> dbt_run >> dbt_test