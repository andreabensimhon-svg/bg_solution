# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold : Agrégations Sales
# MAGIC Ventes, analyse clients et suivi grands comptes

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# Chargement des tables Silver
df_orders = spark.read.table("silver.clean_sales_orders")
df_lines = spark.read.table("silver.clean_sales_distributions")
df_clients = spark.read.table("silver.clean_clients")
df_produits = spark.read.table("silver.clean_produits")

# COMMAND ----------

# ========================
# FACT_VENTES
# ========================

df_ventes_detail = (
    df_orders
    .join(df_lines, "commande_id", "inner")
    .join(df_clients.select("client_id", "type_client"), "client_id", "left")
    .join(df_produits.select("produit_id", "categorie"), "produit_id", "left")
)

df_gold_ventes = (
    df_ventes_detail
    .withColumn("annee", F.year("date_commande"))
    .withColumn("mois", F.month("date_commande"))
    .withColumn("trimestre", F.quarter("date_commande"))
    .withColumn("date_vente", F.make_date("annee", "mois", F.lit(1)))
    .groupBy(
        "date_vente", "annee", "mois", "trimestre",
        "client_id", "type_client", "produit_id",
        F.col("categorie").alias("categorie_produit"),
        "canal_vente", "commercial_id"
    )
    .agg(
        F.countDistinct("commande_id").alias("nb_commandes"),
        F.sum("quantite").alias("quantite_totale"),
        F.sum("montant_ligne_ht").alias("ca_brut_ht"),
        F.sum(
            F.col("montant_ligne_ht") * F.col("remise_ligne_pct") / 100
        ).alias("remise_totale")
    )
    .withColumn("ca_net_ht", F.col("ca_brut_ht") - F.col("remise_totale"))
    .withColumn("panier_moyen", F.round(F.col("ca_net_ht") / F.col("nb_commandes"), 2))
    .withColumn("_computed_ts", F.current_timestamp())
)

df_gold_ventes.write.format("delta").mode("overwrite").saveAsTable("gold.fact_ventes")
print(f"✅ Gold fact_ventes : {df_gold_ventes.count()} lignes")

# COMMAND ----------

# ========================
# FACT_CLIENTS (segmentation ABC + scoring)
# ========================

df_client_agg = (
    df_orders
    .groupBy("client_id")
    .agg(
        F.min("date_commande").alias("date_premier_achat"),
        F.max("date_commande").alias("date_dernier_achat"),
        F.countDistinct("commande_id").alias("nb_commandes_total"),
        F.sum("montant_apres_remise").alias("ca_cumule_ht")
    )
)

# CA 12 derniers mois
df_ca_12m = (
    df_orders
    .filter(F.col("date_commande") >= F.add_months(F.current_date(), -12))
    .groupBy("client_id")
    .agg(F.sum("montant_apres_remise").alias("ca_12_mois"))
)

# Segmentation ABC sur le CA cumulé
window_abc = Window.orderBy(F.col("ca_cumule_ht").desc())

df_gold_clients = (
    df_client_agg
    .join(df_ca_12m, "client_id", "left")
    .join(df_clients.select("client_id", "raison_sociale", "type_client", "ville", "pays"), "client_id", "left")
    .withColumn("ca_12_mois", F.coalesce("ca_12_mois", F.lit(0)))
    .withColumn("panier_moyen", F.round(F.col("ca_cumule_ht") / F.col("nb_commandes_total"), 2))
    # ABC : top 20% = A, 20-50% = B, reste = C
    .withColumn("_rank_pct", F.percent_rank().over(window_abc))
    .withColumn("categorie_abc", F.when(F.col("_rank_pct") <= 0.2, "A")
                                  .when(F.col("_rank_pct") <= 0.5, "B")
                                  .otherwise("C"))
    # Score fidélité simple (combinaison fréquence + récence + montant)
    .withColumn("score_fidelite", F.round(
        (F.col("nb_commandes_total") * 0.3 +
         F.datediff(F.current_date(), "date_dernier_achat") * -0.01 +
         F.col("ca_cumule_ht") / 10000 * 0.4), 2
    ))
    .drop("_rank_pct")
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "client_id", "raison_sociale", "type_client", "ville", "pays",
        "date_premier_achat", "date_dernier_achat",
        "nb_commandes_total", "ca_cumule_ht", "ca_12_mois",
        "panier_moyen", "categorie_abc", "score_fidelite",
        "_computed_ts"
    )
)

df_gold_clients.write.format("delta").mode("overwrite").saveAsTable("gold.fact_clients")
print(f"✅ Gold fact_clients : {df_gold_clients.count()} lignes")

# COMMAND ----------

# ========================
# DIM_CLIENTS_GRANDS_COMPTES
# ========================

# Grands comptes = clients de catégorie A
df_grands_comptes_ids = df_gold_clients.filter(F.col("categorie_abc") == "A").select("client_id")

df_gc_mensuel = (
    df_orders
    .join(df_grands_comptes_ids, "client_id", "inner")
    .withColumn("annee", F.year("date_commande"))
    .withColumn("mois", F.month("date_commande"))
    .withColumn("date_periode", F.make_date("annee", "mois", F.lit(1)))
    .groupBy("client_id", "date_periode", "annee", "mois")
    .agg(
        F.sum("montant_apres_remise").alias("ca_mensuel_ht"),
        F.count("*").alias("nb_commandes")
    )
)

# CA cumulé année en cours
window_cum = Window.partitionBy("client_id", "annee").orderBy("mois").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_gc_enrichi = (
    df_gc_mensuel
    .withColumn("ca_cumul_annee", F.sum("ca_mensuel_ht").over(window_cum))
    .withColumn("objectif_mensuel", F.col("ca_mensuel_ht") * 1.1)  # +10% croissance cible
    .withColumn("atteinte_objectif_pct", F.round(
        F.col("ca_mensuel_ht") / F.col("objectif_mensuel") * 100, 2
    ))
    .join(df_gold_clients.select("client_id", "raison_sociale"), "client_id", "left")
    .withColumn("top_produit", F.lit(None).cast("string"))  # À enrichir
    .withColumn("satisfaction_score", F.lit(None).cast("decimal(5,2)"))
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "client_id", "raison_sociale", "date_periode", "annee", "mois",
        "ca_mensuel_ht", "ca_cumul_annee",
        "objectif_mensuel", "atteinte_objectif_pct",
        "nb_commandes", "top_produit", "satisfaction_score",
        "_computed_ts"
    )
)

df_gc_enrichi.write.format("delta").mode("overwrite").saveAsTable("gold.dim_clients_grands_comptes")
print(f"✅ Gold dim_clients_grands_comptes : {df_gc_enrichi.count()} lignes")

# COMMAND ----------

# ========================
# DIM_PRODUCTS (enrichi avec métriques ventes)
# ========================

df_product_metrics = (
    df_lines
    .groupBy("produit_id")
    .agg(
        F.sum("quantite").alias("nb_ventes_total"),
        F.sum("montant_ligne_ht").alias("ca_total_ht")
    )
)

window_rank = Window.orderBy(F.col("ca_total_ht").desc())

df_gold_products = (
    df_produits
    .join(df_product_metrics, "produit_id", "left")
    .withColumn("nb_ventes_total", F.coalesce("nb_ventes_total", F.lit(0)))
    .withColumn("ca_total_ht", F.coalesce("ca_total_ht", F.lit(0)))
    .withColumn("marge_moyenne_pct", F.lit(35.0))  # Placeholder
    .withColumn("classement_ventes", F.row_number().over(window_rank))
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "produit_id", "nom_produit", "categorie", "sous_categorie",
        "reference", "prix_catalogue_ht", "unite_vente", "poids_kg",
        "nb_ventes_total", "ca_total_ht", "marge_moyenne_pct",
        "classement_ventes", "is_active", "_computed_ts"
    )
)

df_gold_products.write.format("delta").mode("overwrite").saveAsTable("gold.dim_products")
print(f"✅ Gold dim_products : {df_gold_products.count()} lignes")
