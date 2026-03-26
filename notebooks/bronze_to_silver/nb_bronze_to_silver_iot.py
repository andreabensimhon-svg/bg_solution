# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver : Nettoyage des données IoT
# MAGIC Mesures capteurs temps réel et métadonnées images

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, TimestampType

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# ========================
# MESURES IoT
# ========================

df_raw_iot = spark.read.table("bronze.raw_iot_stream")

df_clean_iot = (
    df_raw_iot
    # Dédoublonnage sur event_id
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("event_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    # Typage
    .withColumn("event_id", F.col("event_id").cast(LongType()))
    .withColumn("valeur", F.col("valeur").cast(DoubleType()))
    .withColumn("timestamp_capteur", F.to_timestamp("timestamp_capteur"))
    .withColumn("qualite_signal", F.col("qualite_signal").cast(DoubleType()))
    # Normalisation
    .withColumn("type_mesure", F.lower(F.trim("type_mesure")))
    .withColumn("unite", F.lower(F.trim("unite")))
    # Détection anomalie simple (valeurs hors plage raisonnable)
    .withColumn("is_anomalie", F.when(
        ((F.col("type_mesure") == "temperature") & 
         ((F.col("valeur") < -50) | (F.col("valeur") > 500))) |
        ((F.col("type_mesure") == "pression") & 
         ((F.col("valeur") < 0) | (F.col("valeur") > 1000))) |
        ((F.col("type_mesure") == "humidite") & 
         ((F.col("valeur") < 0) | (F.col("valeur") > 100))),
        F.lit(True)
    ).otherwise(F.lit(False)))
    # Validation
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("capteur_id").isNotNull())
    .filter(F.col("valeur").isNotNull())
    .filter(F.col("timestamp_capteur").isNotNull())
    # Filtrage des mesures trop anciennes (> 30 jours)
    .filter(F.col("timestamp_capteur") >= F.date_sub(F.current_date(), 30))
    # Métadonnées
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
    .select(
        "event_id", "capteur_id", "type_mesure",
        "valeur", "unite", "timestamp_capteur",
        "ligne_production", "zone", "qualite_signal",
        "is_anomalie",
        "_cleaned_ts", "_source_batch_id"
    )
)

df_clean_iot.write.format("delta").mode("overwrite").saveAsTable("silver.clean_iot_measures")
print(f"✅ Silver clean_iot_measures : {df_clean_iot.count()} lignes")
print(f"   dont {df_clean_iot.filter(F.col('is_anomalie')).count()} anomalies détectées")
