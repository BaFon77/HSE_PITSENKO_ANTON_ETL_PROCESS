#!/usr/bin/env python3

import json
import random
import string
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

REGIONS        = ["DE-HE", "DE-BY", "DE-BE", "FR-IDF", "FR-ARA", "PL-MZ", "ES-MD", "IT-LOM", "NL-NH", "SE-AB", "RU-MOW", "US-CA"]
RISK_LEVELS    = ["low", "medium", "high"]
DECISION_STATI = ["approved", "manual_review", "rejected"]
DOC_TYPES      = ["passport", "driver_license", "snils", "inn", "salary_statement"]
DOC_STATUSES   = ["verified", "pending", "rejected"]


def make_message(idx):
    base_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    submitted = base_date + timedelta(
        days=random.randint(0, 150),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    documents = [
        {"type": random.choice(DOC_TYPES), "status": random.choice(DOC_STATUSES)}
        for _ in range(random.randint(1, 4))
    ]
    notes = "".join(random.choices(string.ascii_letters + " ", k=random.randint(80, 300)))
    return json.dumps({
        "application_id": f"loan_{idx:07d}",
        "customer": {
            "customer_id": f"cust_{random.randint(100, 99999)}",
            "region":      random.choice(REGIONS),
            "age":         random.randint(18, 75),
            "income":      round(random.uniform(20000, 500000), 2),
            "employment":  random.choice(["employed", "self_employed", "retired", "student"]),
        },
        "loan": {
            "amount":      random.randint(5000, 5_000_000),
            "term_months": random.choice([6, 12, 18, 24, 36, 48, 60, 84, 120]),
            "purpose":     random.choice(["mortgage", "auto", "consumer", "business", "education"]),
            "currency":    random.choice(["RUB", "USD", "EUR"]),
        },
        "scoring": {
            "score":      random.randint(300, 850),
            "risk_level": random.choice(RISK_LEVELS),
            "pd":         round(random.uniform(0.001, 0.35), 4),
            "lgd":        round(random.uniform(0.1, 0.9), 4),
        },
        "documents":       documents,
        "decision_status": random.choice(DECISION_STATI),
        "submitted_at":    submitted.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "notes":           notes,
    }, ensure_ascii=False)


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

    target_bytes = 22 * 1024 * 1024
    messages = []
    total = 0
    idx = 1
    while total < target_bytes:
        msg = make_message(idx)
        messages.append((msg,))
        total += len(msg.encode("utf-8"))
        idx += 1

    df = spark.createDataFrame(messages, ["value"])

    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-pbp66vbg7t7q09gj.mdb.yandexcloud.net:9091") \
        .option("topic", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .save()

    spark.stop()


if __name__ == "__main__":
    main()