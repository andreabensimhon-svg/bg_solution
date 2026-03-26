# Databricks notebook source
# MAGIC %md
# MAGIC # ML - Détection d'anomalies IoT (lh_iot)
# MAGIC K-Means clustering + distance au centre

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import numpy as np

# COMMAND ----------

df_iot = spark.read.table("lh_iot.silver.clean_iot_measures")

df_pivot = (
    df_iot.groupBy("capteur_id", "ligne_production", "zone",
                    F.window("timestamp_capteur", "5 minutes").alias("window"))
    .pivot("type_mesure", ["temperature", "pression", "humidite"])
    .agg(F.avg("valeur").alias("avg"), F.stddev("valeur").alias("std"))
)

feature_cols = ["temperature_avg", "temperature_std", "pression_avg", "pression_std", "humidite_avg", "humidite_std"]
for col in feature_cols:
    median = df_pivot.approxQuantile(col, [0.5], 0.01)
    if median:
        df_pivot = df_pivot.fillna({col: median[0]})

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
df_features = assembler.transform(df_pivot)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
scaler_model = scaler.fit(df_features)
df_scaled = scaler_model.transform(df_features)

kmeans = KMeans(k=3, featuresCol="features", predictionCol="cluster", seed=42)
kmeans_model = kmeans.fit(df_scaled)
df_clustered = kmeans_model.transform(df_scaled)

centers = kmeans_model.clusterCenters()

from pyspark.sql.types import DoubleType
@F.udf(DoubleType())
def distance_to_center(features, cluster):
    center = centers[cluster]
    return float(np.sqrt(np.sum((np.array(features) - np.array(center)) ** 2)))

df_dist = df_clustered.withColumn("distance_centre", distance_to_center(F.col("features"), F.col("cluster")))
threshold = df_dist.approxQuantile("distance_centre", [0.95], 0.01)[0]

df_result = (
    df_dist.filter(F.col("distance_centre") > threshold)
    .withColumn("severite", F.when(F.col("distance_centre") > threshold * 1.5, "critical").otherwise("warning"))
    .withColumn("modele_detection", F.lit("kmeans_distance"))
    .withColumn("_computed_ts", F.current_timestamp())
    .select("capteur_id", F.col("window.start").alias("timestamp_detection"),
            "temperature_avg", "pression_avg", "humidite_avg", "distance_centre",
            "severite", "ligne_production", "zone", "modele_detection", "_computed_ts")
)

df_result.write.format("delta").mode("overwrite").saveAsTable("lh_iot.gold.fact_iot_anomalies_ml")
print(f"✅ Anomalies ML : {df_result.count()} lignes")

kmeans_model.save("Files/ml_models/iot_anomaly_kmeans")
scaler_model.save("Files/ml_models/iot_anomaly_scaler")
