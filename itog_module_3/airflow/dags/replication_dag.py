import os
import pandas as pd
from bson import ObjectId
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from sqlalchemy.types import DateTime, Text, Numeric

COLLECTIONS = ["Restaurants", "Customers", "DeliveryDrivers", "Orders", "OrderReviews"]
DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))

def clean_collection_data(df: pd.DataFrame, collection_name: str) -> pd.DataFrame:
    """Функция очистки данных"""
    id_col = f"{collection_name.lower()}_id"
    if id_col in df.columns:
        df.drop_duplicates(subset=[id_col], keep='first', inplace=True)

    for col in ['order_amount', 'rating']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'order_amount' in df.columns:
        df = df[df['order_amount'] > 0]

    critical_ids = ['customer_id', 'restaurant_id', 'order_id', 'driver_id']
    cols_to_check = [c for c in critical_ids if c in df.columns]
    if cols_to_check:
        df.dropna(subset=cols_to_check, inplace=True)
        for col in cols_to_check:
            bad_vals = ['unknown', 'none', 'nan', '', 'null']
            df = df[~df[col].astype(str).str.lower().isin(bad_vals)]

    text_cols = df.select_dtypes(include=['object']).columns
    df[text_cols] = df[text_cols].fillna("unknown")

    return df

def replicate_collection(collection_name: str):
    mongo_hook = MongoHook(conn_id="mongo_default")
    db = mongo_hook.get_conn()["food_delivery"]
    data = list(db[collection_name].find())

    if not data:
        return

    df = pd.DataFrame(data)
    df = df.applymap(lambda x: str(x) if isinstance(x, ObjectId) else x)

    if "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)

    df = clean_collection_data(df, collection_name)

    pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")
    engine = pg_hook.get_sqlalchemy_engine()
    main_table = collection_name.lower()
    if collection_name == "DeliveryDrivers": main_table = "delivery_drivers"

    column_types = {}
    for col in df.columns:
        if any(x in col.lower() for x in ["time", "date", "joined", "created"]):
            df[col] = pd.to_datetime(df[col], errors="coerce")
            column_types[col] = DateTime
        elif col in ['order_amount', 'rating']:
            if main_table == 'orderreviews':
                column_types[col] = Text
            else:
                column_types[col] = Numeric
        else:
            column_types[col] = Text

    pg_hook.run(f"TRUNCATE TABLE {main_table} CASCADE;")
    df_main = df.drop(columns=[c for c in df.columns if isinstance(df[c].iloc[0], list)] if not df.empty else [])
    df_main.to_sql(main_table, engine, if_exists="append", index=False, dtype=column_types)

    if collection_name == "Customers" and 'actions' in df.columns:
        actions_list = []
        for i, row in df.iterrows():
            customer_id = row['customer_id']
            for a in row['actions']:
                actions_list.append({
                    'customer_id': customer_id,
                    'action_type': a.get('action', 'unknown'),
                    'action_timestamp': pd.to_datetime(a.get('timestamp'))
                })
        if actions_list:
            df_actions = pd.DataFrame(actions_list)
            pg_hook.run("TRUNCATE TABLE customer_actions CASCADE;")
            df_actions.to_sql(
                'customer_actions',
                engine,
                if_exists='append',
                index=False,
                dtype={
                    'customer_id': Text,
                    'action_type': Text,
                    'action_timestamp': DateTime
                }
            )

with DAG(
        dag_id="mongo_to_postgres_analytic",
        start_date=datetime(2024, 3, 8),
        schedule_interval="@daily",
        template_searchpath=DAG_FOLDER,
        catchup=False
) as dag:

    prepare_db = PostgresOperator(
        task_id="prepare_database",
        postgres_conn_id="postgres_dwh",
        sql="sql/init_db.sql"
    )

    tasks = [PythonOperator(
        task_id=f"replicate_{coll.lower()}",
        python_callable=replicate_collection,
        op_args=[coll]
    ) for coll in COLLECTIONS]

    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_analytics",
        trigger_dag_id="food_delivery_marts"
    )

    prepare_db >> tasks[0]
    for i in range(len(tasks) - 1): tasks[i] >> tasks[i+1]
    tasks[-1] >> trigger_marts