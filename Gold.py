# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, year, month, current_timestamp

gold_schema = "gold_schema"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")

dim_user = spark.table(f"{catalog_name}.bronze_schema.dimuser")
dim_track = spark.table(f"{catalog_name}.bronze_schema.dimtrack")
dim_artist = spark.table(f"{catalog_name}.bronze_schema.dimartist")
fact_stream = spark.table(f"{catalog_name}.bronze_schema.factstream")

fact_enriched = fact_stream.alias("f") \
    .join(dim_user.alias("u"), col("f.user_id") == col("u.user_id"), "left") \
    .join(dim_track.alias("t"), col("f.track_id") == col("t.track_id"), "left") \
    .join(dim_artist.alias("a"), col("t.artist_id") == col("a.artist_id"), "left") \
    .select(
        col("f.stream_id"),
        col("f.user_id"),
        col("f.track_id"),
        col("f.date_key"),
        col("f.listen_duration"),
        col("f.device_type"),
        col("f.stream_timestamp"),
        col("u.user_name").alias("user_name"),
        col("u.country").alias("user_country"),
        col("u.subscription_type").alias("user_subscription_type"),
        col("t.track_name"),
        col("t.album_name"),
        col("t.duration_sec").alias("track_duration_sec"),
        col("t.release_date"),
        col("a.artist_name"),
        col("a.genre").alias("artist_genre"),
        col("a.country").alias("artist_country")
    ) \
    .withColumn("year", year(col("stream_timestamp"))) \
    .withColumn("month", month(col("stream_timestamp"))) \
    .withColumn("gold_ingestion_ts", current_timestamp())


gold_fact_table = f"{catalog_name}.{gold_schema}.factstream_gold"

fact_enriched.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable(gold_fact_table)

print(f"Gold layer FactStream created successfully at {gold_fact_table}!")

spark.table(gold_fact_table).printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE spotify.gold_schema.factstream_gold;