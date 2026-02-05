from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pymssql

def export_sqlserver_data():
    conn = pymssql.connect(
        server="192.168.1.100",
        user="sa",
        password="123",
        database="pubs",
        port=1433
    )

    query = "SELECT id, name FROM dbo.sql_test;"
    df = pd.read_sql(query, conn)
    conn.close()

    output_file = "/opt/airflow/dags/sqlserver_export.csv"
    df.to_csv(output_file, index=False)
    print(f"FILE SAVED: {output_file}")

with DAG(
    dag_id="sqlserver_to_file_pymssql_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    export_task = PythonOperator(
        task_id="export_task",
        python_callable=export_sqlserver_data
    )
