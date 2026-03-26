# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver : Nettoyage des données Sales
# MAGIC Commandes, lignes de commande et distributions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType, DateType

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# ========================
# COMMANDES VENTES
# ========================

df_raw_sales = spark.read.table("bronze.raw_erp_sales")

df_clean_orders = (
    df_raw_sales
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("commande_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    # Typage
    .withColumn("commande_id", F.col("commande_id").cast(IntegerType()))
    .withColumn("client_id", F.col("client_id").cast(IntegerType()))
    .withColumn("date_commande", F.to_date("date_commande", "yyyy-MM-dd"))
    .withColumn("date_livraison", F.to_date("date_livraison", "yyyy-MM-dd"))
    .withColumn("montant_total_ht", F.col("montant_total_ht").cast(DecimalType(12, 2)))
    .withColumn("remise_pct", F.coalesce(F.col("remise_pct").cast(DecimalType(5, 2)), F.lit(0)))
    .withColumn("commercial_id", F.col("commercial_id").cast(IntegerType()))
    # Calcul montant après remise
    .withColumn("montant_apres_remise", F.round(
        F.col("montant_total_ht") * (1 - F.col("remise_pct") / 100), 2
    ))
    # Normalisation
    .withColumn("statut_commande", F.lower(F.trim("statut_commande")))
    .withColumn("canal_vente", F.lower(F.trim("canal_vente")))
    # Validation
    .filter(F.col("commande_id").isNotNull())
    .filter(F.col("client_id").isNotNull())
    .filter(F.col("date_commande").isNotNull())
    # Métadonnées
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
)

# Ajout nombre de lignes par commande
df_raw_lines = spark.read.table("bronze.raw_sales_lines")
df_nb_lignes = (
    df_raw_lines
    .groupBy("commande_id")
    .agg(F.countDistinct("ligne_id").alias("nb_lignes"))
)

df_clean_orders = (
    df_clean_orders
    .join(df_nb_lignes, "commande_id", "left")
    .withColumn("nb_lignes", F.coalesce(F.col("nb_lignes"), F.lit(0)))
    .select(
        "commande_id", "client_id", "date_commande", "date_livraison",
        "statut_commande", "montant_total_ht", "remise_pct",
        "montant_apres_remise", "commercial_id", "canal_vente", "nb_lignes",
        "_cleaned_ts", "_source_batch_id"
    )
)

df_clean_orders.write.format("delta").mode("overwrite").saveAsTable("silver.clean_sales_orders")
print(f"✅ Silver clean_sales_orders : {df_clean_orders.count()} lignes")

# COMMAND ----------

# ========================
# LIGNES DE COMMANDE (DISTRIBUTIONS)
# ========================

df_clean_lines = (
    df_raw_lines
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("ligne_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    # Typage
    .withColumn("ligne_id", F.col("ligne_id").cast(IntegerType()))
    .withColumn("commande_id", F.col("commande_id").cast(IntegerType()))
    .withColumn("produit_id", F.col("produit_id").cast(IntegerType()))
    .withColumn("quantite", F.col("quantite").cast(IntegerType()))
    .withColumn("prix_unitaire_ht", F.col("prix_unitaire_ht").cast(DecimalType(10, 2)))
    .withColumn("remise_ligne_pct", F.coalesce(
        F.col("remise_ligne_pct").cast(DecimalType(5, 2)), F.lit(0)
    ))
    # Calcul montant ligne
    .withColumn("montant_ligne_ht", F.round(
        F.col("quantite") * F.col("prix_unitaire_ht") * (1 - F.col("remise_ligne_pct") / 100), 2
    ))
    # Validation
    .filter(F.col("ligne_id").isNotNull())
    .filter(F.col("commande_id").isNotNull())
    .filter(F.col("produit_id").isNotNull())
    .filter(F.col("quantite") > 0)
    # Métadonnées
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
    .select(
        "ligne_id", "commande_id", "produit_id",
        "quantite", "prix_unitaire_ht", "remise_ligne_pct",
        "montant_ligne_ht",
        "_cleaned_ts", "_source_batch_id"
    )
)

df_clean_lines.write.format("delta").mode("overwrite").saveAsTable("silver.clean_sales_distributions")
print(f"✅ Silver clean_sales_distributions : {df_clean_lines.count()} lignes")
