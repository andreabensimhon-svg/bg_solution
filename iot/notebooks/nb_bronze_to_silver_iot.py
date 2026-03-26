# Databricks notebook source
# MAGIC %md
# MAGIC # IoT : Bronze → Silver (lh_iot)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType

# COMMAND ----------

df = spark.read.table("lh_iot.bronze.raw_iot_stream")

df_clean = (
    df.withColumn("_rn", F.row_number().over(Window.partitionBy("event_id").orderBy(F.col("_ingestion_ts").desc())))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("event_id", F.col("event_id").cast(LongType()))
    .withColumn("valeur", F.col("valeur").cast(DoubleType()))
    .withColumn("timestamp_capteur", F.to_timestamp("timestamp_capteur"))
    .withColumn("qualite_signal", F.col("qualite_signal").cast(DoubleType()))
    .withColumn("type_mesure", F.lower(F.trim("type_mesure")))
    .withColumn("unite", F.lower(F.trim("unite")))
    .withColumn("is_anomalie", F.when(
        ((F.col("type_mesure") == "temperature") & ((F.col("valeur") < -50) | (F.col("valeur") > 500))) |
        ((F.col("type_mesure") == "pression") & ((F.col("valeur") < 0) | (F.col("valeur") > 1000))) |
        ((F.col("type_mesure") == "humidite") & ((F.col("valeur") < 0) | (F.col("valeur") > 100))),
        F.lit(True)).otherwise(F.lit(False)))
    .filter(F.col("event_id").isNotNull() & F.col("valeur").isNotNull() & F.col("timestamp_capteur").isNotNull())
    .filter(F.col("timestamp_capteur") >= F.date_sub(F.current_date(), 30))
    .withColumn("_cleaned_ts", F.current_timestamp()).withColumn("_source_batch_id", F.col("_batch_id"))
    .select("event_id", "capteur_id", "type_mesure", "valeur", "unite", "timestamp_capteur",
            "ligne_production", "zone", "qualite_signal", "is_anomalie",
            "_cleaned_ts", "_source_batch_id")
)

df_clean.write.format("delta").mode("overwrite").saveAsTable("lh_iot.silver.clean_iot_measures")
print(f"✅ clean_iot_measures : {df_clean.count()} lignes")
print(f"   dont {df_clean.filter(F.col('is_anomalie')).count()} anomalies")
