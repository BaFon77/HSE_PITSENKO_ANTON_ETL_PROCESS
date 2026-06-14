from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "hadoopbacket"
INPUT_PATH = f"s3a://{BUCKET}/engine/"
OUTPUT_PATH = f"s3a://{BUCKET}/engine_processed/"
OUTPUT_TABLE = "engine_aggregated"

spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("mergeSchema", "true") \
    .parquet(INPUT_PATH)

df_clean = df.filter(
    (F.col("Engine_RPM_RPM").isNotNull()) &
    (F.col("Engine_RPM_RPM") > 0) &
    (F.col("Vehicle_Speed_km_per_h").isNotNull()) &
    (F.col("Vehicle_Speed_km_per_h") > 0)
)

df_enriched = df_clean \
    .withColumn(
    "fuel_consumption_proxy",
    F.col("MAF_g_per_sec") / F.col("Vehicle_Speed_km_per_h") * F.lit(100.0)
) \
    .withColumn(
    "driving_mode",
    F.when(
        (F.col("Vehicle_Speed_km_per_h") > 100) &
        (F.col("Absolute_Load_pct") > 60),
        "aggressive"
    ).when(
        (F.col("Vehicle_Speed_km_per_h").between(60, 100)) &
        (F.col("Absolute_Load_pct").between(20, 60)),
        "cruising"
    ).when(
        F.col("Vehicle_Speed_km_per_h") < 30,
        "urban"
    ).otherwise("mixed")
) \
    .withColumn(
    "fuel_trim_anomaly",
    F.when(
        (F.col("Short_Term_Fuel_Trim_Bank_1_pct").isNotNull()) &
        (F.abs(F.col("Short_Term_Fuel_Trim_Bank_1_pct")) > 10),
        F.lit(True)
    ).otherwise(F.lit(False))
)

df_agg = df_enriched.groupBy("EngineType", "Generalized_Weight", "driving_mode") \
    .agg(
    F.count("*").alias("record_count"),
    F.round(F.avg("Engine_RPM_RPM"), 1).alias("avg_rpm"),
    F.round(F.max("Engine_RPM_RPM"), 1).alias("max_rpm"),
    F.round(F.avg("Vehicle_Speed_km_per_h"), 2).alias("avg_speed_kmh"),
    F.round(F.max("Vehicle_Speed_km_per_h"), 2).alias("max_speed_kmh"),
    F.round(F.avg("MAF_g_per_sec"), 4).alias("avg_maf"),
    F.round(F.avg("log_MAF"), 4).alias("avg_log_maf"),
    F.round(F.avg("Absolute_Load_pct"), 2).alias("avg_load_pct"),
    F.round(F.avg("OAT_DegC"), 2).alias("avg_oat_c"),
    F.round(F.avg("Short_Term_Fuel_Trim_Bank_1_pct"), 4).alias("avg_stft_b1"),
    F.round(F.avg("Long_Term_Fuel_Trim_Bank_1_pct"), 4).alias("avg_ltft_b1"),
    F.round(F.avg("Short_Term_Fuel_Trim_Bank_2_pct"), 4).alias("avg_stft_b2"),
    F.round(F.avg("Long_Term_Fuel_Trim_Bank_2_pct"), 4).alias("avg_ltft_b2"),
    F.round(F.avg("fuel_consumption_proxy"), 4).alias("avg_fuel_consumption_proxy"),
    F.sum(F.col("fuel_trim_anomaly").cast("int")).alias("fuel_trim_anomaly_count"),
) \
    .orderBy("EngineType", "Generalized_Weight", "driving_mode")

df_agg.write \
    .mode("overwrite") \
    .option("path", OUTPUT_PATH) \
    .saveAsTable(OUTPUT_TABLE)

spark.stop()
