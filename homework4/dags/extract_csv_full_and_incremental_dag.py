from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.db import provide_session
from airflow.models import DagRun
from datetime import datetime, timedelta
import pandas as pd

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"
FULL_DB_TABLE = "extract_demo.temperature_data"
INCR_DB_TABLE = "extract_demo.temperature_data_incremental"

@provide_session
def choose_branch(dag_run, session=None):
    runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_run.dag_id)
        .count()
    )

    if runs == 1:
        return "process_full"

    return "process_incremental"

def process_temperature_data(mode, days=None):
    df = pd.read_csv(DATA_PATH)
    df = df[df["out/in"] == "In"]

    df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")

    if mode == "incremental":
        cutoff_date = datetime.now() - timedelta(days=days)
        df = df[df["noted_date"] >= cutoff_date]

    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df = df[(df["temp"] >= p5) & (df["temp"] <= p95)]

    df["noted_date"] = df["noted_date"].dt.date

    daily_df = df.groupby(["noted_date", "out/in"], as_index=False)["temp"].mean().round(3)
    
    daily_df["year"] = pd.to_datetime(daily_df["noted_date"]).dt.year

    hottest = daily_df.sort_values(["year", "temp"], ascending=[True, False]).groupby("year").head(5)
    coldest = daily_df.sort_values(["year", "temp"], ascending=[True, True]).groupby("year").head(5)

    return pd.concat([hottest, coldest]).drop(columns=["year"])

def save_to_postgres(ti, mode):
    task_id = "process_full" if mode == "full" else "process_incremental"
    df = ti.xcom_pull(task_ids=task_id)
    rows = df.to_dict(orient='records')
    table = FULL_DB_TABLE if mode == "full" else INCR_DB_TABLE

    postgres_hook = PostgresHook(postgres_conn_id='demo_db')

    if mode == "full":
        postgres_hook.run(f"TRUNCATE TABLE {table}")
    
    rows = df.rename(columns={'out/in': 'out_in'}).to_dict(orient='records')
    values = [[row['noted_date'], row['temp'], row['out_in']] for row in rows]

    postgres_hook.insert_rows(
        table=table,
        rows=values,
        target_fields=['noted_date', 'temp', 'out_in']
    )

with DAG(
    dag_id="extract_csv_full_and_incremental",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
) as dag:
    
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_branch,
    )

    process_full = PythonOperator(
        task_id="process_full",
        python_callable=process_temperature_data,
        op_kwargs={"mode": "full"},
    )

    save_full = PythonOperator(
        task_id="save_full",
        python_callable=save_to_postgres,
        op_kwargs={"mode": "full"},
    )

    process_incremental = PythonOperator(
        task_id="process_incremental",
        python_callable=process_temperature_data,
        op_kwargs={"mode": "incremental", "days": 3},
    )

    save_incremental = PythonOperator(
        task_id="save_incremental",
        python_callable=save_to_postgres,
        op_kwargs={"mode": "incremental"},
    )

    branch >> process_full >> save_full
    branch >> process_incremental >> save_incremental