from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="extract",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
) as dag:
    history = PostgresOperator(
        task_id='extract_json',
        postgres_conn_id='demo_db',
        sql="""
        create table if not exists extract_demo.data_from_json as
        select
            post.value->>'name' as name,
            post.value->>'species' as species,
            post.value->>'favFoods' as favFoods,
            post.value->>'birthYear' as birthYear,
            post.value->>'photo' as photo
        from extract_demo.json_content,
        jsonb_array_elements(json_data->'pets') as post(value);
        """
    )

    extract_xml = PostgresOperator(
        task_id="extract_xml",
        postgres_conn_id="demo_db",
        sql="""
        create table if not exists extract_demo.data_from_xml as
        select
            (xpath('/food/name/text()', food))[1]::text                as name,
            (xpath('/food/mfr/text()', food))[1]::text                 as manufacturer,
            (xpath('/food/serving/text()', food))[1]::text::numeric    as serving_value,
            (xpath('/food/serving/@units', food))[1]::text             as serving_units,
            (xpath('/food/calories/@total', food))[1]::text::int       as calories_total,
            (xpath('/food/calories/@fat', food))[1]::text::int         as calories_fat,
            (xpath('/food/total-fat/text()', food))[1]::text::numeric      as total_fat,
            (xpath('/food/saturated-fat/text()', food))[1]::text::numeric as saturated_fat,
            (xpath('/food/cholesterol/text()', food))[1]::text::int        as cholesterol,
            (xpath('/food/sodium/text()', food))[1]::text::int             as sodium,
            (xpath('/food/carb/text()', food))[1]::text::numeric           as carbs,
            (xpath('/food/fiber/text()', food))[1]::text::numeric          as fiber,
            (xpath('/food/protein/text()', food))[1]::text::numeric        as protein,
            (xpath('/food/vitamins/a/text()', food))[1]::text::int     as vitamin_a,
            (xpath('/food/vitamins/c/text()', food))[1]::text::int     as vitamin_c,
            (xpath('/food/minerals/ca/text()', food))[1]::text::int    as calcium,
            (xpath('/food/minerals/fe/text()', food))[1]::text::int    as iron
        from (select unnest(xpath('/nutrition/food', xml_content)) as food
            from extract_demo.xml_content) 
        AS query;
        """
    )