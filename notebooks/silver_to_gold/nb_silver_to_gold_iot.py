# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold : Agrégations IoT
# MAGIC Mesures capteurs agrégées et détection d'anomalies

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# ========================
# FACT_IOT_REALTIME (agrégation par fenêtre temporelle)
# ========================

df_iot = spark.read.table("silver.clean_iot_measures")

# Agrégation par fenêtre de 5 minutes
df_gold_iot = (
    df_iot
    .withColumn("window_start", F.window("timestamp_capteur", "5 minutes").start)
    .withColumn("window_end", F.window("timestamp_capteur", "5 minutes").end)
    .groupBy("window_start", "window_end", "capteur_id", "type_mesure", "ligne_production", "zone")
    .agg(
        F.round(F.avg("valeur"), 4).alias("valeur_moyenne"),
        F.round(F.min("valeur"), 4).alias("valeur_min"),
        F.round(F.max("valeur"), 4).alias("valeur_max"),
        F.round(F.stddev("valeur"), 4).alias("ecart_type"),
        F.count("*").alias("nb_mesures"),
        F.round(F.avg("qualite_signal"), 2).alias("qualite_signal_moy"),
        F.max("is_anomalie").alias("has_anomalie")
    )
    .withColumn("_computed_ts", F.current_timestamp())
)

df_gold_iot.write.format("delta").mode("overwrite").saveAsTable("gold.fact_iot_realtime")
print(f"✅ Gold fact_iot_realtime : {df_gold_iot.count()} lignes")

# COMMAND ----------

# ========================
# FACT_IOT_ANOMALIES
# ========================

# Seuils par type de mesure
seuils = {
    "temperature": {"min": -10, "max": 200, "unite": "°C"},
    "pression":    {"min": 0.5, "max": 500, "unite": "bar"},
    "humidite":    {"min": 5,   "max": 95,  "unite": "%"}
}

# Détection par écart IQR (interquartile range)
window_capteur = Window.partitionBy("capteur_id", "type_mesure")

df_with_stats = (
    df_iot
    .withColumn("q1", F.percentile_approx("valeur", 0.25).over(window_capteur))
    .withColumn("q3", F.percentile_approx("valeur", 0.75).over(window_capteur))
    .withColumn("iqr", F.col("q3") - F.col("q1"))
    .withColumn("lower_bound", F.col("q1") - 1.5 * F.col("iqr"))
    .withColumn("upper_bound", F.col("q3") + 1.5 * F.col("iqr"))
    .withColumn("valeur_attendue", (F.col("q1") + F.col("q3")) / 2)
)

df_anomalies = (
    df_with_stats
    .filter(
        (F.col("valeur") < F.col("lower_bound")) |
        (F.col("valeur") > F.col("upper_bound"))
    )
    .withColumn("ecart_pct", F.round(
        F.abs(F.col("valeur") - F.col("valeur_attendue")) / F.col("valeur_attendue") * 100, 2
    ))
    .withColumn("severite", F.when(F.col("ecart_pct") > 50, "critical")
                             .when(F.col("ecart_pct") > 20, "warning")
                             .otherwise("info"))
    .withColumn("modele_detection", F.lit("iqr_1.5"))
    .withColumn("is_confirmee", F.lit(False))
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "capteur_id", "type_mesure",
        F.col("timestamp_capteur").alias("timestamp_detection"),
        F.col("valeur").alias("valeur_anomalie"),
        "valeur_attendue", "ecart_pct", "severite",
        "ligne_production", "zone",
        "modele_detection", "is_confirmee",
        "_computed_ts"
    )
)

df_anomalies.write.format("delta").mode("overwrite").saveAsTable("gold.fact_iot_anomalies")
print(f"✅ Gold fact_iot_anomalies : {df_anomalies.count()} lignes")
print(f"   - Critical: {df_anomalies.filter(F.col('severite') == 'critical').count()}")
print(f"   - Warning:  {df_anomalies.filter(F.col('severite') == 'warning').count()}")
print(f"   - Info:     {df_anomalies.filter(F.col('severite') == 'info').count()}")
