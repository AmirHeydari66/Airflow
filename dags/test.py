from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import sqlite3


# -----------------------------
# Step 1: Extract
# -----------------------------
def extract_data():
    print("Reading sales.csv file...")
    df = pd.read_csv('/opt/airflow/data/sales.csv')
    df.to_csv('/opt/airflow/data/temp_sales.csv', index=False)


# -----------------------------
# Step 2: Transform
# -----------------------------
def transform_data():
    print("Cleaning and aggregating data...")
    df = pd.read_csv('/opt/airflow/data/temp_sales.csv')

    # حذف رکوردهای null
    df = df.dropna()

    # محاسبه مجموع فروش
    total_sales = df['amount'].sum()

    result = pd.DataFrame({
        "date": [datetime.now().date()],
        "total_sales": [total_sales]
    })

    result.to_csv('/opt/airflow/data/final_sales.csv', index=False)


# -----------------------------
# Step 3: Load
# -----------------------------
def load_to_db():
    print("Loading into database...")
    conn = sqlite3.connect('/opt/airflow/data/sales.db')
    df = pd.read_csv('/opt/airflow/data/final_sales.csv')
    df.to_sql('daily_sales', conn, if_exists='append', index=False)
    conn.close()


# -----------------------------
# Define DAG
# -----------------------------
with DAG(
    dag_id='daily_sales_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 2 * * *',  # هر روز ساعت 02:00
    catchup=False,
    tags=['ETL', 'sales'],
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db
    )

    end = EmptyOperator(task_id='end')

    # ترتیب اجرا
    start >> extract >> transform >> load >> end
