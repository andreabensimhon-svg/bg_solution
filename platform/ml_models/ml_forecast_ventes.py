# Databricks notebook source
# MAGIC %md
# MAGIC # ML - Forecast Ventes (lh_sales → lh_finance)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
from prophet import Prophet

# COMMAND ----------

df_ventes = spark.read.table("lh_sales.gold.fact_ventes")
df_monthly = (
    df_ventes.groupBy("annee", "mois")
    .agg(F.sum("ca_net_ht").alias("ca_net_ht"))
    .withColumn("date", F.make_date("annee", "mois", F.lit(1)))
    .orderBy("date")
)

pdf = df_monthly.select(F.col("date").alias("ds"), F.col("ca_net_ht").alias("y")).toPandas()
pdf["ds"] = pd.to_datetime(pdf["ds"])

model = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False, interval_width=0.80)
model.fit(pdf)

future = model.make_future_dataframe(periods=12, freq="MS")
forecast = model.predict(future)

pdf_fc = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
pdf_fc.columns = ["date_forecast", "ca_prevu", "intervalle_confiance_bas", "intervalle_confiance_haut"]
pdf_fc = pdf_fc.merge(pdf.rename(columns={"ds": "date_forecast", "y": "ca_reel"}), on="date_forecast", how="left")
pdf_fc["ecart_pct"] = ((pdf_fc["ca_reel"] - pdf_fc["ca_prevu"]) / pdf_fc["ca_prevu"] * 100).round(2)

df_spark = spark.createDataFrame(pdf_fc)
df_final = (
    df_spark
    .withColumn("horizon_mois", F.lit(12))
    .withColumn("annee_cible", F.year("date_forecast"))
    .withColumn("mois_cible", F.month("date_forecast"))
    .withColumn("categorie_produit", F.lit("tous"))
    .withColumn("modele_utilise", F.lit("prophet_v1"))
    .withColumn("_computed_ts", F.current_timestamp())
)

# Écriture cross-domaine : résultat dans lh_finance
df_final.write.format("delta").mode("overwrite").saveAsTable("lh_finance.gold.fact_forecast")
print(f"✅ Forecast → lh_finance.gold.fact_forecast : {df_final.count()} lignes")
