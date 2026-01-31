# Databricks notebook source
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

jdbc_hostname = "spotify-db.c7cmw2844uny.ap-south-1.rds.amazonaws.com"
jdbc_port = 3306
database_name = "spotify"

jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{database_name}?useSSL=false"

username = "admin"        
password = "DjOmkar8856"

catalog_name = "spotify"
schema_name = "bronze_schema"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")


tables_to_load = [
    "DimArtist",
    "DimDate",
    "DimTrack",
    "DimUser",
    "FactStream"
]

primary_keys = {
    "DimArtist": "artist_id",
    "DimDate": "date_key",
    "DimTrack": "track_id",
    "DimUser": "user_id",
    "FactStream": "stream_id"
}

incremental_cols = {
    "DimArtist": "updated_at",
    "DimDate": None,           
    "DimTrack": "updated_at",
    "DimUser": "updated_at",
    "FactStream": "stream_timestamp"
}

def bronze_etl(source_table: str, primary_key: str, incremental_col: str = None):

    target_table = f"{catalog_name}.{schema_name}.{source_table.lower()}"
    
    table_exists = spark.catalog.tableExists(target_table)
    print(f"\nProcessing table: {source_table} → {target_table}")
    print(f"Bronze table exists: {table_exists}")
    

    if (not table_exists) or (incremental_col is None):
        print(f"Running FULL LOAD for {source_table}...")
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", source_table)
            .option("user", username)
            .option("password", password)
            .load()
        )

        df = df.withColumn("ingestion_ts", current_timestamp())
        
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)
        
        print(f"Full load completed for {source_table}.")
        df.show(5)
    
    else:
        print(f"Running INCREMENTAL LOAD for {source_table}...")
        
    
        last_ts = spark.sql(f"""
            SELECT COALESCE(MAX({incremental_col}), '1900-01-01')
            FROM {target_table}
        """).collect()[0][0]
        
        print(f"Last processed {incremental_col}: {last_ts}")
        
 
        incr_query = f"""
        (SELECT *
         FROM {source_table}
         WHERE {incremental_col} > '{last_ts}') AS inc
        """
        incr_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", incr_query)
            .option("user", username)
            .option("password", password)
            .load()
        )
        
        incr_count = incr_df.limit(1).count()
        if incr_count == 0:
            print(f"No new records for {source_table}. Skipping incremental load.")
            return
        
        incr_df = incr_df.withColumn("ingestion_ts", current_timestamp())
        
        delta_table = DeltaTable.forName(spark, target_table)
        (
            delta_table.alias("t")
            .merge(incr_df.alias("s"), f"t.{primary_key} = s.{primary_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        print(f"Incremental merge completed for {source_table}.")
        incr_df.show(5)


for table in tables_to_load:
    bronze_etl(
        source_table=table,
        primary_key=primary_keys[table],
        incremental_col=incremental_cols[table]
    )
