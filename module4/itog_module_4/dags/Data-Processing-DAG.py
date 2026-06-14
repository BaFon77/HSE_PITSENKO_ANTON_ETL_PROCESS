import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Данные вашей инфраструктуры
YC_DP_AZ = ''
YC_DP_SSH_PUBLIC_KEY = ''
YC_DP_SUBNET_ID = ''
YC_DP_SA_ID = ''
YC_DP_METASTORE_URI = ''
YC_BUCKET = 'hadoopbacket'

# Настройки DAG
with DAG(
        'DATA_INGEST',
        schedule='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    # 1 этап: создание кластера Yandex Data Proc
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-hdd',
        masternode_disk_size=64,
        computenode_resource_preset='m2.small',
        computenode_disk_type='network-hdd',
        computenode_disk_size=64,
        computenode_count=2,
        computenode_max_hosts_count=2,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    # 2 этап: запуск задания PySpark
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/create-table.py',
    )

    # 3 этап: удаление кластера Yandex Data Processing
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Формирование DAG из указанных выше этапов
    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster