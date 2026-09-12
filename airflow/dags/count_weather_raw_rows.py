from airflow.sdk import task, DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

with DAG(
    dag_id="postgres_hook_example",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def count_raw_rows():

        hook = PostgresHook(
            postgres_conn_id='flight_db'
        )

        records = hook.get_records(
            """
            SELECT COUNT(*)
            FROM raw.weather_raw
            """
        )

        row_count = records[0][0]

        print(f"Количество строк: {row_count}")

        return row_count

    count_raw_rows()
