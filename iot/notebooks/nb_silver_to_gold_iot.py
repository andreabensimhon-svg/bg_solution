# Databricks notebook source
# MAGIC %md
# MAGIC # IoT : Silver → Gold (lh_iot) - Agrégation + Anomalies

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

df_iot = spark.read.table("lh_iot.silver.clean_iot_measures")

# === FACT_IOT_REALTIME (fenêtre 5 min) ===
df_gold = (
    df_iot.withColumn("window_start", F.window("timestamp_capteur", "5 minutes").start)
    .withColumn("window_end", F.window("timestamp_capteur", "5 minutes").end)
    .groupBy("window_start", "window_end", "capteur_id", "type_mesure", "ligne_production", "zone")
    .agg(F.round(F.avg("valeur"), 4).alias("valeur_moyenne"),
         F.round(F.min("valeur"), 4).alias("valeur_min"),
         F.round(F.max("valeur"), 4).alias("valeur_max"),
         F.round(F.stddev("valeur"), 4).alias("ecart_type"),
         F.count("*").alias("nb_mesures"),
         F.round(F.avg("qualite_signal"), 2).alias("qualite_signal_moy"),
         F.max("is_anomalie").alias("has_anomalie"))
    .withColumn("_computed_ts", F.current_timestamp())
)
df_gold.write.format("delta").mode("overwrite").saveAsTable("lh_iot.gold.fact_iot_realtime")
print(f"✅ fact_iot_realtime : {df_gold.count()} lignes")

# COMMAND ----------

# === FACT_IOT_ANOMALIES (IQR) ===
w = Window.partitionBy("capteur_id", "type_mesure")
df_stats = (
    df_iot
    .withColumn("q1", F.percentile_approx("valeur", 0.25).over(w))
    .withColumn("q3", F.percentile_approx("valeur", 0.75).over(w))
    .withColumn("iqr", F.col("q3") - F.col("q1"))
    .withColumn("lower", F.col("q1") - 1.5 * F.col("iqr"))
    .withColumn("upper", F.col("q3") + 1.5 * F.col("iqr"))
    .withColumn("valeur_attendue", (F.col("q1") + F.col("q3")) / 2)
)

df_anomalies = (
    df_stats.filter((F.col("valeur") < F.col("lower")) | (F.col("valeur") > F.col("upper")))
    .withColumn("ecart_pct", F.round(F.abs(F.col("valeur") - F.col("valeur_attendue")) / F.col("valeur_attendue") * 100, 2))
    .withColumn("severite", F.when(F.col("ecart_pct") > 50, "critical").when(F.col("ecart_pct") > 20, "warning").otherwise("info"))
    .withColumn("modele_detection", F.lit("iqr_1.5"))
    .withColumn("is_confirmee", F.lit(False))
    .withColumn("_computed_ts", F.current_timestamp())
    .select("capteur_id", "type_mesure", F.col("timestamp_capteur").alias("timestamp_detection"),
            F.col("valeur").alias("valeur_anomalie"), "valeur_attendue", "ecart_pct", "severite",
            "ligne_production", "zone", "modele_detection", "is_confirmee", "_computed_ts")
)
df_anomalies.write.format("delta").mode("overwrite").saveAsTable("lh_iot.gold.fact_iot_anomalies")
print(f"✅ fact_iot_anomalies : {df_anomalies.count()} lignes")
