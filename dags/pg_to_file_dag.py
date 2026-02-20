from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import os


def export_from_postgres_to_file():

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cur = conn.cursor()

    query = """
        SELECT dag_id, root_dag_id, is_paused, is_subdag, is_active, last_parsed_time
        FROM public.dag;
    """

    cur.execute(query)
    rows = cur.fetchall()

    # مسیر فولدر DAG ها (داخل کانتینر Airflow)
    dags_folder = os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")

    output_path = os.path.join(dags_folder, "pg_output.txt")

    with open(output_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(str(r) + "\n")

    print(">>> FILE SAVED:", output_path)

    cur.close()
    conn.close()


with DAG(
    dag_id="pg_to_file_txt_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval= "*/2 * * * *",
    catchup=False
):

    export_task = PythonOperator(
        task_id="export_task",
        python_callable=export_from_postgres_to_file
    )

    export_task
