# Databricks notebook source
# MAGIC %md
# MAGIC # ML - Forecast Ventes
# MAGIC Prévision des ventes avec Prophet via PySpark

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd

# COMMAND ----------

# Chargement des données de ventes Gold
df_ventes = spark.read.table("gold.fact_ventes")

# Agrégation mensuelle
df_monthly = (
    df_ventes
    .groupBy("annee", "mois")
    .agg(
        F.sum("ca_net_ht").alias("ca_net_ht"),
        F.sum("nb_commandes").alias("nb_commandes"),
        F.sum("quantite_totale").alias("quantite_totale")
    )
    .withColumn("date", F.make_date("annee", "mois", F.lit(1)))
    .orderBy("date")
)

# COMMAND ----------

# Conversion en Pandas pour Prophet
pdf_monthly = df_monthly.select(
    F.col("date").alias("ds"),
    F.col("ca_net_ht").alias("y")
).toPandas()

pdf_monthly["ds"] = pd.to_datetime(pdf_monthly["ds"])

# COMMAND ----------

# Entraînement du modèle Prophet
from prophet import Prophet

model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10,
    interval_width=0.80
)

model.fit(pdf_monthly)

# COMMAND ----------

# Prévision sur 12 mois
future = model.make_future_dataframe(periods=12, freq="MS")
forecast = model.predict(future)

# Sélection des colonnes pertinentes
pdf_forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
pdf_forecast.columns = ["date_forecast", "ca_prevu", "intervalle_confiance_bas", "intervalle_confiance_haut"]

# Ajout des données réelles pour comparaison
pdf_forecast = pdf_forecast.merge(
    pdf_monthly.rename(columns={"ds": "date_forecast", "y": "ca_reel"}),
    on="date_forecast",
    how="left"
)

# Calcul de l'écart
pdf_forecast["ecart_pct"] = (
    (pdf_forecast["ca_reel"] - pdf_forecast["ca_prevu"]) /
    pdf_forecast["ca_prevu"] * 100
).round(2)

# COMMAND ----------

# Conversion en Spark DataFrame et écriture dans Gold
schema = StructType([
    StructField("date_forecast", DateType()),
    StructField("ca_prevu", DoubleType()),
    StructField("intervalle_confiance_bas", DoubleType()),
    StructField("intervalle_confiance_haut", DoubleType()),
    StructField("ca_reel", DoubleType()),
    StructField("ecart_pct", DoubleType())
])

df_forecast_spark = spark.createDataFrame(pdf_forecast, schema)

df_forecast_final = (
    df_forecast_spark
    .withColumn("horizon_mois", F.lit(12))
    .withColumn("annee_cible", F.year("date_forecast"))
    .withColumn("mois_cible", F.month("date_forecast"))
    .withColumn("categorie_produit", F.lit("tous"))
    .withColumn("modele_utilise", F.lit("prophet_v1"))
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "date_forecast", "horizon_mois", "annee_cible", "mois_cible",
        "categorie_produit", "ca_prevu", "ca_reel", "ecart_pct",
        "intervalle_confiance_bas", "intervalle_confiance_haut",
        "modele_utilise", "_computed_ts"
    )
)

df_forecast_final.write.format("delta").mode("overwrite").saveAsTable("gold.fact_forecast")
print(f"✅ Forecast ventes : {df_forecast_final.count()} lignes (dont {df_forecast_final.filter(F.col('ca_reel').isNull()).count()} prévisions futures)")

# COMMAND ----------

# Sauvegarde du modèle sérialisé
import pickle
with open("/lakehouse/default/Files/ml_models/forecast_prophet_model.pkl", "wb") as f:
    pickle.dump(model, f)
print("✅ Modèle Prophet sauvegardé")
