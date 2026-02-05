from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pyodbc


def export_sqlserver_data():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=mssql,1433;"
        "DATABASE=pubs;"
        "UID=sa;"
        "PWD=123;"
    )
    conn = pyodbc.connect(conn_str)

    query = "SELECT id, name FROM dbo.sql_test;"
    df = pd.read_sql(query, conn)
    conn.close()

    output_file = "/opt/airflow/dags/sqlserver_export.csv"
    df.to_csv(output_file, index=False)
    print(f"FILE SAVED: {output_file}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="sqlserver_to_file_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    export_task = PythonOperator(
        task_id="export_task",
        python_callable=export_sqlserver_data
    )
