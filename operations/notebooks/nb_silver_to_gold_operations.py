# Databricks notebook source
# MAGIC %md
# MAGIC # Operations : Silver → Gold
# MAGIC KPIs production, supply chain et sanitaire (lh_operations)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# === FACT_PRODUCTION ===
df_prod = spark.read.table("lh_operations.silver.clean_operations_production")
df_produits = spark.read.table("lh_operations.shortcuts.shared_dim_products")

df_gold_prod = (
    df_prod.withColumn("date_production", F.to_date("date_debut"))
    .withColumn("annee", F.year("date_production")).withColumn("mois", F.month("date_production"))
    .withColumn("semaine", F.weekofyear("date_production"))
    .groupBy("annee", "mois", "semaine", "ligne_production", "produit_id")
    .agg(F.min("date_production").alias("date_production"), F.count("*").alias("nb_ordres"),
         F.sum("quantite_prevue").alias("quantite_prevue_total"),
         F.sum("quantite_produite").alias("quantite_produite_total"),
         F.round(F.avg("taux_rendement"), 2).alias("taux_rendement_moyen"),
         F.round(F.sum("duree_heures"), 2).alias("duree_totale_heures"),
         F.sum(F.when(F.col("statut") == "annule", 1).otherwise(0)).alias("nb_incidents"))
    .join(df_produits.select("produit_id", F.col("categorie").alias("categorie_produit")), "produit_id", "left")
    .withColumn("_computed_ts", F.current_timestamp())
)
df_gold_prod.write.format("delta").mode("overwrite").saveAsTable("lh_operations.gold.fact_production")
print(f"✅ fact_production : {df_gold_prod.count()} lignes")

# COMMAND ----------

# === FACT_SUPPLY_CHAIN ===
df_sup = spark.read.table("lh_operations.silver.clean_supply_chain")
df_gold_sup = (
    df_sup.withColumn("annee", F.year("date_commande")).withColumn("mois", F.month("date_commande"))
    .withColumn("date_periode", F.make_date("annee", "mois", F.lit(1)))
    .groupBy("annee", "mois", "date_periode", "fournisseur_id", "produit_id")
    .agg(F.count("*").alias("nb_commandes"), F.sum("quantite").alias("quantite_totale"),
         F.sum(F.col("quantite") * F.col("prix_unitaire")).alias("montant_total"),
         F.round(F.avg(F.abs("retard_jours")), 1).alias("delai_moyen_jours"),
         F.round(F.sum(F.when(F.col("retard_jours") > 0, 1).otherwise(0)) / F.count("*") * 100, 2).alias("taux_retard_pct"),
         F.round(F.sum(F.when(F.col("statut") == "livre", 1).otherwise(0)) / F.count("*") * 100, 2).alias("taux_conformite_pct"))
    .join(df_produits.select("produit_id", F.col("categorie").alias("categorie_produit")), "produit_id", "left")
    .withColumn("_computed_ts", F.current_timestamp())
)
df_gold_sup.write.format("delta").mode("overwrite").saveAsTable("lh_operations.gold.fact_supply_chain")
print(f"✅ fact_supply_chain : {df_gold_sup.count()} lignes")

# COMMAND ----------

# === FACT_PROCESS_SANITAIRE ===
df_san = spark.read.table("lh_operations.silver.clean_process_sanitaire")
df_gold_san = (
    df_san.withColumn("date_p", F.to_date("date_controle"))
    .withColumn("annee", F.year("date_p")).withColumn("mois", F.month("date_p"))
    .withColumn("date_periode", F.make_date("annee", "mois", F.lit(1)))
    .groupBy("annee", "mois", "date_periode", "ligne_production", "type_controle")
    .agg(F.count("*").alias("nb_controles"),
         F.sum(F.when(F.col("resultat") == "conforme", 1).otherwise(0)).alias("nb_conformes"),
         F.sum(F.when(F.col("resultat") == "non_conforme", 1).otherwise(0)).alias("nb_non_conformes"),
         F.sum(F.when(F.col("resultat") == "alerte", 1).otherwise(0)).alias("nb_alertes"),
         F.round(F.avg("mesure_valeur"), 4).alias("valeur_moyenne"),
         F.round(F.min("mesure_valeur"), 4).alias("valeur_min"),
         F.round(F.max("mesure_valeur"), 4).alias("valeur_max"))
    .withColumn("taux_conformite_pct", F.round(F.col("nb_conformes") / F.col("nb_controles") * 100, 2))
    .withColumn("_computed_ts", F.current_timestamp())
)
df_gold_san.write.format("delta").mode("overwrite").saveAsTable("lh_operations.gold.fact_process_sanitaire")
print(f"✅ fact_process_sanitaire : {df_gold_san.count()} lignes")
