# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when

# ======================================
# SPARK SESSION
# ======================================
spark = SparkSession.builder.appName("Silver_ETL").getOrCreate()

# Catalog & schema
bronze_catalog = "spotify"
bronze_schema = "bronze_schema"
silver_schema = "silver_schema"

# Create Silver schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{silver_schema}")

# Tables
tables_to_process = ["DimArtist", "DimDate", "DimTrack", "DimUser", "FactStream"]

# ======================================
# SILVER ETL FUNCTION
# ======================================
def silver_etl(table_name):
    bronze_table = f"{bronze_catalog}.{bronze_schema}.{table_name.lower()}"
    silver_table = f"{bronze_catalog}.{silver_schema}.{table_name.lower()}"

    print(f"\nProcessing Bronze → Silver: {table_name}")

    # Read Bronze
    df = spark.table(bronze_table)

    # ------------------------
    # DimArtist
    # ------------------------
    if table_name == "DimArtist":
        silver_df = (
            df
            .filter(col("artist_id").isNotNull())
            .withColumn("artist_name", col("artist_name"))
            .withColumn("ingestion_ts", col("ingestion_ts"))
            .dropDuplicates(["artist_id"])
        )

    # ------------------------
    # DimDate
    # ------------------------
    elif table_name == "DimDate":
        silver_df = (
            df
            .withColumn("date", col("date").cast("date"))
            .dropDuplicates(["date_key"])
        )

    # ------------------------
    # DimTrack
    # ------------------------
    elif table_name == "DimTrack":
        silver_df = (
            df
            .filter(col("track_id").isNotNull() & col("artist_id").isNotNull())
            .withColumn("release_date", col("release_date").cast("date"))
            .dropDuplicates(["track_id"])
        )

    # ------------------------
    # DimUser
    # ------------------------
    elif table_name == "DimUser":
        silver_df = (
            df
            .filter(col("user_id").isNotNull())
            .withColumn("start_date", col("start_date").cast("date"))
            .withColumn("end_date", col("end_date").cast("date"))
            .withColumn("subscription_type", when(col("subscription_type").isNull(), "Free").otherwise(col("subscription_type")))
            .dropDuplicates(["user_id"])
        )

    # ------------------------
    # FactStream
    # ------------------------
    elif table_name == "FactStream":
        silver_df = (
            df
            .filter(col("stream_id").isNotNull() & (col("listen_duration") > 0))
            .withColumn("listen_minutes", (col("listen_duration") / 60).cast("double"))
            .withColumn("stream_timestamp", col("stream_timestamp").cast("timestamp"))
            .dropDuplicates(["stream_id"])
            # Partition by date_key for performance (optional)
        )

    # Add Silver ingestion timestamp
    silver_df = silver_df.withColumn("silver_load_ts", current_timestamp())

    # Write to Silver Delta
    silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    print(f"Silver load completed for {table_name}.")
    silver_df.show(5)
    silver_df.printSchema()


for tbl in tables_to_process:
    silver_etl(tbl)
