# Databricks notebook source
# MAGIC %md
# MAGIC # Sales : Silver → Gold (lh_sales)
# MAGIC Utilise Shortcuts vers lh_shared (dim_clients, dim_products)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

df_orders = spark.read.table("lh_sales.silver.clean_sales_orders")
df_lines = spark.read.table("lh_sales.silver.clean_sales_distributions")
df_clients = spark.read.table("lh_sales.shortcuts.shared_dim_clients")
df_produits = spark.read.table("lh_sales.shortcuts.shared_dim_products")

# === FACT_VENTES ===
df_detail = (
    df_orders.join(df_lines, "commande_id", "inner")
    .join(df_clients.select("client_id", "type_client"), "client_id", "left")
    .join(df_produits.select("produit_id", F.col("categorie").alias("categorie_produit")), "produit_id", "left")
)

df_gold = (
    df_detail.withColumn("annee", F.year("date_commande")).withColumn("mois", F.month("date_commande"))
    .withColumn("trimestre", F.quarter("date_commande"))
    .withColumn("date_vente", F.make_date("annee", "mois", F.lit(1)))
    .groupBy("date_vente", "annee", "mois", "trimestre", "client_id", "type_client",
             "produit_id", "categorie_produit", "canal_vente", "commercial_id")
    .agg(F.countDistinct("commande_id").alias("nb_commandes"), F.sum("quantite").alias("quantite_totale"),
         F.sum("montant_ligne_ht").alias("ca_brut_ht"),
         F.sum(F.col("montant_ligne_ht") * F.col("remise_ligne_pct") / 100).alias("remise_totale"))
    .withColumn("ca_net_ht", F.col("ca_brut_ht") - F.col("remise_totale"))
    .withColumn("panier_moyen", F.round(F.col("ca_net_ht") / F.col("nb_commandes"), 2))
    .withColumn("_computed_ts", F.current_timestamp())
)
df_gold.write.format("delta").mode("overwrite").saveAsTable("lh_sales.gold.fact_ventes")
print(f"✅ fact_ventes : {df_gold.count()} lignes")

# COMMAND ----------

# === DIM_CLIENTS_GRANDS_COMPTES ===
df_gc_ids = df_clients.filter(F.col("categorie_abc") == "A").select("client_id", "raison_sociale")
df_gc = (
    df_orders.join(df_gc_ids, "client_id", "inner")
    .withColumn("annee", F.year("date_commande")).withColumn("mois", F.month("date_commande"))
    .withColumn("date_periode", F.make_date("annee", "mois", F.lit(1)))
    .groupBy("client_id", "raison_sociale", "date_periode", "annee", "mois")
    .agg(F.sum("montant_apres_remise").alias("ca_mensuel_ht"), F.count("*").alias("nb_commandes"))
    .withColumn("ca_cumul_annee", F.sum("ca_mensuel_ht").over(
        Window.partitionBy("client_id", "annee").orderBy("mois").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    .withColumn("objectif_mensuel", F.col("ca_mensuel_ht") * 1.1)
    .withColumn("atteinte_objectif_pct", F.round(F.col("ca_mensuel_ht") / F.col("objectif_mensuel") * 100, 2))
    .withColumn("top_produit", F.lit(None).cast("string"))
    .withColumn("_computed_ts", F.current_timestamp())
)
df_gc.write.format("delta").mode("overwrite").saveAsTable("lh_sales.gold.dim_clients_grands_comptes")
print(f"✅ dim_clients_grands_comptes : {df_gc.count()} lignes")
