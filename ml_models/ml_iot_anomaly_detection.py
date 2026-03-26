# Databricks notebook source
# MAGIC %md
# MAGIC # ML - Détection d'anomalies IoT
# MAGIC Modèle Isolation Forest pour détecter les anomalies capteurs

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import numpy as np

# COMMAND ----------

# Chargement des données IoT Silver
df_iot = spark.read.table("silver.clean_iot_measures")

# Pivot pour avoir une ligne par capteur/timestamp avec toutes les mesures
df_pivot = (
    df_iot
    .groupBy("capteur_id", "ligne_production", "zone",
             F.window("timestamp_capteur", "5 minutes").alias("window"))
    .pivot("type_mesure", ["temperature", "pression", "humidite"])
    .agg(
        F.avg("valeur").alias("avg"),
        F.stddev("valeur").alias("std"),
        F.count("*").alias("count")
    )
)

# COMMAND ----------

# Feature engineering
feature_cols = [
    "temperature_avg", "temperature_std",
    "pression_avg", "pression_std",
    "humidite_avg", "humidite_std"
]

# Remplir les nulls avec la médiane
for col in feature_cols:
    median_val = df_pivot.approxQuantile(col, [0.5], 0.01)
    if median_val:
        df_pivot = df_pivot.fillna({col: median_val[0]})

# COMMAND ----------

# Vectorisation et normalisation
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
df_features = assembler.transform(df_pivot)

scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

# COMMAND ----------

# K-Means pour clustering (les outliers seront les plus éloignés des centres)
kmeans = KMeans(k=3, featuresCol="features", predictionCol="cluster", seed=42)
kmeans_model = kmeans.fit(df_scaled)
df_clustered = kmeans_model.transform(df_scaled)

# COMMAND ----------

# Calcul de la distance au centre du cluster
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import DoubleType

centers = kmeans_model.clusterCenters()

@F.udf(DoubleType())
def distance_to_center(features, cluster):
    center = centers[cluster]
    return float(np.sqrt(np.sum((np.array(features) - np.array(center)) ** 2)))

df_with_distance = df_clustered.withColumn(
    "distance_centre",
    distance_to_center(F.col("features"), F.col("cluster"))
)

# COMMAND ----------

# Seuil d'anomalie : percentile 95 de la distance
threshold = df_with_distance.approxQuantile("distance_centre", [0.95], 0.01)[0]

df_anomalies = (
    df_with_distance
    .withColumn("is_anomalie_ml", F.col("distance_centre") > threshold)
    .withColumn("severite", F.when(F.col("distance_centre") > threshold * 1.5, "critical")
                             .when(F.col("distance_centre") > threshold, "warning")
                             .otherwise("normal"))
    .withColumn("modele_detection", F.lit("kmeans_distance"))
    .withColumn("_computed_ts", F.current_timestamp())
)

# COMMAND ----------

# Sauvegarde des résultats
df_result = df_anomalies.filter(F.col("is_anomalie_ml")).select(
    "capteur_id",
    F.col("window.start").alias("timestamp_detection"),
    "temperature_avg", "pression_avg", "humidite_avg",
    "distance_centre", "severite",
    "ligne_production", "zone",
    "modele_detection", "_computed_ts"
)

df_result.write.format("delta").mode("overwrite").saveAsTable("gold.fact_iot_anomalies_ml")
print(f"✅ Anomalies ML détectées : {df_result.count()}")
print(f"   - Critical: {df_result.filter(F.col('severite') == 'critical').count()}")
print(f"   - Warning: {df_result.filter(F.col('severite') == 'warning').count()}")

# COMMAND ----------

# Sauvegarde du modèle
kmeans_model.save("Files/ml_models/iot_anomaly_kmeans")
scaler_model.save("Files/ml_models/iot_anomaly_scaler")
print("✅ Modèles sauvegardés dans le Lakehouse")
