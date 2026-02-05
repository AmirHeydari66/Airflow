from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2


def export_airflow_metadata():
    conn = psycopg2.connect(
        host="postgres",        # اسم سرویس Postgres در Docker Compose
        port=5432,
        database="airflow",     # دیتابیس internal Airflow
        user="airflow",
        password="airflow"
    )

    query = """
        SELECT dag_id, root_dag_id, is_paused, is_subdag, is_active, last_parsed_time
        FROM public.dag;
    """

    df = pd.read_sql(query, conn)
    conn.close()

    output_file = "/opt/airflow/dags/airflow_dag_list.csv"
    df.to_csv(output_file, index=False)
    print(f"FILE SAVED: {output_file}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="read_airflow_pg_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    export_task = PythonOperator(
        task_id="export_airflow_metadata",
        python_callable=export_airflow_metadata
    )
