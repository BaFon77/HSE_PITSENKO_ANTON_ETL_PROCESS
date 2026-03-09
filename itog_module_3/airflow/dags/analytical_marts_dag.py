from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

POSTGRES_CONN_ID = "postgres_dwh"

with DAG(
        dag_id="food_delivery_marts",
        start_date=datetime(2024, 3, 8),
        schedule_interval=None,
        catchup=False
) as dag:

    create_customer_activity_mart = PostgresOperator(
        task_id="build_customer_activity_mart",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS mart_customer_activity;
            CREATE TABLE mart_customer_activity AS
            SELECT
                customer_id,
                COUNT(order_id) as total_orders,
                SUM(order_amount) as total_spent,
                AVG(order_amount) as avg_check,
                MAX(order_time) as last_order_timestamp
            FROM orders
            WHERE order_amount > 0
            GROUP BY customer_id;
            """
    )

    create_restaurant_performance_mart = PostgresOperator(
        task_id="build_restaurant_performance_mart",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS mart_restaurant_performance;
            CREATE TABLE mart_restaurant_performance AS
            SELECT
                restaurant_id,
                COUNT(order_id) as total_orders,
                SUM(order_amount) as total_revenue,
                COUNT(CASE WHEN status = 'Доставлен' THEN 1 END) as successful_orders
            FROM orders
            WHERE order_amount > 0
            GROUP BY restaurant_id;
            """
    )

    [create_customer_activity_mart, create_restaurant_performance_mart]