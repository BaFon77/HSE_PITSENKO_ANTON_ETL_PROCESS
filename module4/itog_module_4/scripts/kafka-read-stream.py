#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-read-stream-flat").getOrCreate()

    query = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-pbp66vbg7t7q09gj.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) AS value") \
        .where(col("value").isNotNull()) \
        .writeStream \
        .trigger(once=True) \
        .queryName("received_messages") \
        .format("memory") \
        .start()

    query.awaitTermination()

    flat = spark.sql("""
                     SELECT get_json_object(value, '$.application_id')       AS application_id,
                            get_json_object(value, '$.customer.customer_id') AS customer_id,
                            get_json_object(value, '$.customer.region')      AS region,
                            get_json_object(value, '$.customer.age')         AS age,
                            get_json_object(value, '$.customer.income')      AS income,
                            get_json_object(value, '$.customer.employment')  AS employment,
                            get_json_object(value, '$.loan.amount')          AS loan_amount,
                            get_json_object(value, '$.loan.term_months')     AS term_months,
                            get_json_object(value, '$.loan.purpose')         AS loan_purpose,
                            get_json_object(value, '$.loan.currency')        AS currency,
                            get_json_object(value, '$.scoring.score')        AS score,
                            get_json_object(value, '$.scoring.risk_level')   AS risk_level,
                            get_json_object(value, '$.scoring.pd')           AS pd,
                            get_json_object(value, '$.scoring.lgd')          AS lgd,
                            get_json_object(value, '$.decision_status')      AS decision_status,
                            get_json_object(value, '$.submitted_at')         AS submitted_at
                     FROM received_messages
                     """)

    flat.write.format("csv").option("header", "true").mode("overwrite").save(
        "s3a://hadoopbacket/kafka-read-stream-flat-output")


if __name__ == "__main__":
    main()
