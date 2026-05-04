from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine

# Configuration constants
CSV_PATH = "/opt/airflow/dags/data/electronics_sales_week11.csv"
TABLE_NAME = "electronics_sales"

# Connection string for the MySQL container
MYSQL_CONN_STR = "mysql+pymysql://root:root@mysql:3306/projectdb?charset=utf8mb4"


def load_sales_data():
    try:
        df = pd.read_csv(CSV_PATH)

        df = df.dropna(how="all")
        df = df.dropna(subset=["OrderID"])
        df = df.drop_duplicates(subset=["OrderID"])

        df["Quantity"] = df["Quantity"].astype(int)
        df["Price"] = df["Price"].astype(int)
        df["OrderDate"] = pd.to_datetime(df["OrderDate"]).dt.date

        df["load_time"] = datetime.now()

        engine = create_engine(MYSQL_CONN_STR)

        df.to_sql(
            TABLE_NAME,
            con=engine,
            if_exists="append",
            index=False
        )

        return len(df)

    except Exception as e:
        print("❌ ERROR:", e)
        raise


# Define the DAG
with DAG(
    dag_id="csv_to_mysql",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["demo"],
) as dag:

    load_task = PythonOperator(
        task_id="load_csv_to_mysql",
        python_callable=load_sales_data   
    )

    def log_success(ti):
        print("📝 Logging success message...")

        # Get row count from previous task
        row_count = ti.xcom_pull(task_ids='load_csv_to_mysql')

        engine = create_engine(MYSQL_CONN_STR)

        with engine.connect() as conn:
            conn.execute(
                """
                INSERT INTO airflow_logs (message, rows_loaded)
                VALUES (%s, %s)
                """,
                ("DAG ran successfully", row_count)
            )

        print(f"✅ Log saved with {row_count} rows")

    log_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success
    )

    load_task >> log_task