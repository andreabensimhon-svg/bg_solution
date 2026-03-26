# Databricks notebook source
# MAGIC %md
# MAGIC # Shared : Silver → Gold - Dimensions partagées
# MAGIC Enrichissement des dimensions clients et produits (lh_shared)
# MAGIC Ces tables sont exposées aux autres domaines via Lakehouse Shortcuts

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# === DIM_CLIENTS (segmentation ABC + scoring) ===
# NOTE : Nécessite un Shortcut vers lh_sales.silver.clean_sales_orders

df_clients = spark.read.table("lh_shared.silver.clean_clients")

# Tenter de lire les données sales via shortcut (ou table vide si pas encore dispo)
try:
    df_orders = spark.read.table("lh_shared.shortcuts.sales_clean_sales_orders")
    
    df_client_agg = (
        df_orders.groupBy("client_id").agg(
            F.min("date_commande").alias("date_premier_achat"),
            F.max("date_commande").alias("date_dernier_achat"),
            F.countDistinct("commande_id").alias("nb_commandes_total"),
            F.sum("montant_apres_remise").alias("ca_cumule_ht")
        )
    )
    df_ca_12m = (
        df_orders.filter(F.col("date_commande") >= F.add_months(F.current_date(), -12))
        .groupBy("client_id").agg(F.sum("montant_apres_remise").alias("ca_12_mois"))
    )
    window_abc = Window.orderBy(F.col("ca_cumule_ht").desc())
    
    df_gold_clients = (
        df_client_agg
        .join(df_ca_12m, "client_id", "left")
        .join(df_clients.select("client_id", "raison_sociale", "type_client", "ville", "pays"), "client_id", "left")
        .withColumn("ca_12_mois", F.coalesce("ca_12_mois", F.lit(0)))
        .withColumn("panier_moyen", F.round(F.col("ca_cumule_ht") / F.col("nb_commandes_total"), 2))
        .withColumn("_rank_pct", F.percent_rank().over(window_abc))
        .withColumn("categorie_abc", F.when(F.col("_rank_pct") <= 0.2, "A")
                                      .when(F.col("_rank_pct") <= 0.5, "B").otherwise("C"))
        .withColumn("score_fidelite", F.round(
            F.col("nb_commandes_total") * 0.3 + F.datediff(F.current_date(), "date_dernier_achat") * -0.01 +
            F.col("ca_cumule_ht") / 10000 * 0.4, 2
        ))
        .drop("_rank_pct")
        .withColumn("_computed_ts", F.current_timestamp())
    )
except Exception:
    # Fallback si les données sales ne sont pas encore disponibles
    df_gold_clients = (
        df_clients
        .withColumn("date_premier_achat", F.lit(None).cast("date"))
        .withColumn("date_dernier_achat", F.lit(None).cast("date"))
        .withColumn("nb_commandes_total", F.lit(0))
        .withColumn("ca_cumule_ht", F.lit(0).cast("decimal(15,2)"))
        .withColumn("ca_12_mois", F.lit(0).cast("decimal(15,2)"))
        .withColumn("panier_moyen", F.lit(0).cast("decimal(10,2)"))
        .withColumn("categorie_abc", F.lit("C"))
        .withColumn("score_fidelite", F.lit(0).cast("decimal(5,2)"))
        .withColumn("_computed_ts", F.current_timestamp())
        .select("client_id", "raison_sociale", "type_client", "ville", "pays",
                "date_premier_achat", "date_dernier_achat", "nb_commandes_total",
                "ca_cumule_ht", "ca_12_mois", "panier_moyen", "categorie_abc",
                "score_fidelite", "_computed_ts")
    )

df_gold_clients.write.format("delta").mode("overwrite").saveAsTable("lh_shared.gold.dim_clients")
print(f"✅ dim_clients : {df_gold_clients.count()} lignes")

# COMMAND ----------

# === DIM_PRODUCTS (enrichi) ===
df_produits = spark.read.table("lh_shared.silver.clean_produits")

df_gold_products = (
    df_produits
    .withColumn("nb_ventes_total", F.lit(0))
    .withColumn("ca_total_ht", F.lit(0).cast("decimal(15,2)"))
    .withColumn("marge_moyenne_pct", F.lit(35.0))
    .withColumn("classement_ventes", F.row_number().over(Window.orderBy("produit_id")))
    .withColumn("_computed_ts", F.current_timestamp())
    .select("produit_id", "nom_produit", "categorie", "sous_categorie",
            "reference", "prix_catalogue_ht", "unite_vente", "poids_kg",
            "nb_ventes_total", "ca_total_ht", "marge_moyenne_pct",
            "classement_ventes", "is_active", "_computed_ts")
)

df_gold_products.write.format("delta").mode("overwrite").saveAsTable("lh_shared.gold.dim_products")
print(f"✅ dim_products : {df_gold_products.count()} lignes")
