from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"
DB_TABLE = "extract_demo.temperature_data"

def process_temperature_data():
    df = pd.read_csv(DATA_PATH)
    df = df[df["out/in"] == "In"]

    df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")

    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df = df[(df["temp"] >= p5) & (df["temp"] <= p95)]

    df["noted_date"] = df["noted_date"].dt.date

    daily_df = df.groupby(["noted_date", "out/in"], as_index=False)["temp"].mean().round(3)
    
    daily_df["year"] = pd.to_datetime(daily_df["noted_date"]).dt.year

    hottest = daily_df.sort_values(["year", "temp"], ascending=[True, False]).groupby("year").head(5)
    coldest = daily_df.sort_values(["year", "temp"], ascending=[True, True]).groupby("year").head(5)

    return pd.concat([hottest, coldest]).drop(columns=["year"])

def save_to_postgres(ti):
    df = ti.xcom_pull(task_ids='process_temperature_data')
    rows = df.to_dict(orient='records')
    
    for row in rows:
        sql = """
        INSERT INTO {} (noted_date, temp, out_in) 
        VALUES ('{}', {}, '{}')
        """.format(
            DB_TABLE,
            row['noted_date'],
            row['temp'],
            row['out/in']
        )
        postgres_hook = PostgresHook(postgres_conn_id='demo_db')
        postgres_hook.run(sql)

with DAG(
    dag_id="extractcsv",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
) as dag:
    
    process_task = PythonOperator(
        task_id="process_temperature_data",
        python_callable=process_temperature_data,
    )

    save_task = PythonOperator(
        task_id="save_to_postgres",
        python_callable=save_to_postgres,
        provide_context=True,
    )

    process_task >> save_task