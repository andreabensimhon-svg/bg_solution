# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold : Agrégations Finance
# MAGIC Marges, forecasts et bilans comptables enrichis

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# ========================
# FACT_MARGES
# ========================

df_transactions = spark.read.table("silver.clean_finance_transactions")
df_clients = spark.read.table("silver.clean_clients")
df_produits = spark.read.table("silver.clean_produits")
df_distributions = spark.read.table("silver.clean_sales_distributions")
df_orders = spark.read.table("silver.clean_sales_orders")

# Jointure commandes + lignes pour avoir le détail produit avec coûts
df_ventes_detail = (
    df_orders
    .join(df_distributions, "commande_id", "inner")
    .join(df_produits, "produit_id", "left")
)

# Calcul des marges par client, produit et mois
df_marges = (
    df_ventes_detail
    .withColumn("annee", F.year("date_commande"))
    .withColumn("mois", F.month("date_commande"))
    .withColumn("trimestre", F.quarter("date_commande"))
    .groupBy("annee", "mois", "trimestre", "client_id", "produit_id", "categorie")
    .agg(
        F.sum("montant_ligne_ht").alias("chiffre_affaires_ht"),
        F.sum(F.col("quantite") * F.col("prix_catalogue_ht") * 0.65).alias("cout_revient"),
        F.sum("quantite").alias("volume_vendu")
    )
    .withColumn("marge_brute", F.col("chiffre_affaires_ht") - F.col("cout_revient"))
    .withColumn("marge_brute_pct", F.round(
        F.col("marge_brute") / F.col("chiffre_affaires_ht") * 100, 2
    ))
    .withColumn("marge_nette", F.col("marge_brute") * 0.85)  # Estimation charges
    .withColumn("marge_nette_pct", F.round(
        F.col("marge_nette") / F.col("chiffre_affaires_ht") * 100, 2
    ))
    .withColumn("date_calcul", F.make_date("annee", "mois", F.lit(1)))
    .withColumnRenamed("categorie", "categorie_produit")
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "date_calcul", "annee", "mois", "trimestre",
        "client_id", "produit_id", "categorie_produit",
        "chiffre_affaires_ht", "cout_revient",
        "marge_brute", "marge_brute_pct",
        "marge_nette", "marge_nette_pct",
        "volume_vendu", "_computed_ts"
    )
)

df_marges.write.format("delta").mode("overwrite").saveAsTable("gold.fact_marges")
print(f"✅ Gold fact_marges : {df_marges.count()} lignes")

# COMMAND ----------

# ========================
# DIM_BILANS_COMPTABLES (enrichis avec variation N-1)
# ========================

df_bilans = spark.read.table("silver.clean_bilans_comptables")

# Self-join pour variation N-1
df_bilans_n1 = (
    df_bilans.alias("n")
    .join(
        df_bilans.alias("n1"),
        (F.col("n.poste_comptable") == F.col("n1.poste_comptable")) &
        (F.col("n.type_bilan") == F.col("n1.type_bilan")) &
        (F.col("n.annee_fiscale") == F.col("n1.annee_fiscale") + 1),
        "left"
    )
    .select(
        F.col("n.bilan_id"),
        F.col("n.annee_fiscale"),
        F.col("n.type_bilan"),
        F.col("n.poste_comptable"),
        F.col("n.libelle"),
        F.col("n.montant"),
        F.col("n1.montant").alias("montant_n_moins_1"),
        F.col("n.devise"),
        F.col("n.date_cloture"),
    )
    .withColumn("variation_pct", F.round(
        F.when(F.col("montant_n_moins_1") != 0,
               ((F.col("montant") - F.col("montant_n_moins_1")) / F.abs("montant_n_moins_1")) * 100)
        .otherwise(None), 2
    ))
    .withColumn("_computed_ts", F.current_timestamp())
)

df_bilans_n1.write.format("delta").mode("overwrite").saveAsTable("gold.dim_bilans_comptables")
print(f"✅ Gold dim_bilans_comptables : {df_bilans_n1.count()} lignes")

# COMMAND ----------

# ========================
# FACT_FORECAST (placeholder - sera enrichi par le modèle ML)
# ========================

df_ca_mensuel = (
    df_transactions
    .filter(F.col("type_transaction") == "facture")
    .withColumn("annee", F.year("date_transaction"))
    .withColumn("mois", F.month("date_transaction"))
    .groupBy("annee", "mois")
    .agg(F.sum("montant_ht").alias("ca_reel"))
)

# Forecast simple : moyenne mobile 3 mois
window_3m = Window.orderBy("annee", "mois").rowsBetween(-3, -1)

df_forecast = (
    df_ca_mensuel
    .withColumn("ca_prevu", F.round(F.avg("ca_reel").over(window_3m), 2))
    .withColumn("ecart_pct", F.round(
        F.when(F.col("ca_prevu") != 0,
               ((F.col("ca_reel") - F.col("ca_prevu")) / F.col("ca_prevu")) * 100)
        .otherwise(None), 2
    ))
    .withColumn("date_forecast", F.current_date())
    .withColumn("horizon_mois", F.lit(3))
    .withColumn("annee_cible", F.col("annee"))
    .withColumn("mois_cible", F.col("mois"))
    .withColumn("categorie_produit", F.lit("tous"))
    .withColumn("intervalle_confiance_bas", F.col("ca_prevu") * 0.9)
    .withColumn("intervalle_confiance_haut", F.col("ca_prevu") * 1.1)
    .withColumn("modele_utilise", F.lit("moyenne_mobile_3m"))
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "date_forecast", "horizon_mois", "annee_cible", "mois_cible",
        "categorie_produit", "ca_prevu", "ca_reel", "ecart_pct",
        "intervalle_confiance_bas", "intervalle_confiance_haut",
        "modele_utilise", "_computed_ts"
    )
)

df_forecast.write.format("delta").mode("overwrite").saveAsTable("gold.fact_forecast")
print(f"✅ Gold fact_forecast : {df_forecast.count()} lignes")
