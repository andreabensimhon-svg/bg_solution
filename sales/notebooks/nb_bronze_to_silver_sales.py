# Databricks notebook source
# MAGIC %md
# MAGIC # Sales : Bronze → Silver (lh_sales)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType

# COMMAND ----------

# === COMMANDES ===
df = spark.read.table("lh_sales.bronze.raw_erp_sales")
df_lines_raw = spark.read.table("lh_sales.bronze.raw_sales_lines")

df_clean = (
    df.withColumn("_rn", F.row_number().over(Window.partitionBy("commande_id").orderBy(F.col("_ingestion_ts").desc())))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("commande_id", F.col("commande_id").cast(IntegerType()))
    .withColumn("client_id", F.col("client_id").cast(IntegerType()))
    .withColumn("date_commande", F.to_date("date_commande", "yyyy-MM-dd"))
    .withColumn("date_livraison", F.to_date("date_livraison", "yyyy-MM-dd"))
    .withColumn("montant_total_ht", F.col("montant_total_ht").cast(DecimalType(12, 2)))
    .withColumn("remise_pct", F.coalesce(F.col("remise_pct").cast(DecimalType(5, 2)), F.lit(0)))
    .withColumn("commercial_id", F.col("commercial_id").cast(IntegerType()))
    .withColumn("montant_apres_remise", F.round(F.col("montant_total_ht") * (1 - F.col("remise_pct") / 100), 2))
    .withColumn("statut_commande", F.lower(F.trim("statut_commande")))
    .withColumn("canal_vente", F.lower(F.trim("canal_vente")))
    .filter(F.col("commande_id").isNotNull() & F.col("client_id").isNotNull())
)

df_nb_lignes = df_lines_raw.groupBy("commande_id").agg(F.countDistinct("ligne_id").alias("nb_lignes"))
df_clean = (
    df_clean.join(df_nb_lignes, "commande_id", "left")
    .withColumn("nb_lignes", F.coalesce("nb_lignes", F.lit(0)))
    .withColumn("_cleaned_ts", F.current_timestamp()).withColumn("_source_batch_id", F.col("_batch_id"))
    .select("commande_id", "client_id", "date_commande", "date_livraison", "statut_commande",
            "montant_total_ht", "remise_pct", "montant_apres_remise", "commercial_id", "canal_vente",
            "nb_lignes", "_cleaned_ts", "_source_batch_id")
)
df_clean.write.format("delta").mode("overwrite").saveAsTable("lh_sales.silver.clean_sales_orders")
print(f"✅ clean_sales_orders : {df_clean.count()} lignes")

# COMMAND ----------

# === LIGNES ===
df_clean_l = (
    df_lines_raw.withColumn("_rn", F.row_number().over(Window.partitionBy("ligne_id").orderBy(F.col("_ingestion_ts").desc())))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("ligne_id", F.col("ligne_id").cast(IntegerType()))
    .withColumn("commande_id", F.col("commande_id").cast(IntegerType()))
    .withColumn("produit_id", F.col("produit_id").cast(IntegerType()))
    .withColumn("quantite", F.col("quantite").cast(IntegerType()))
    .withColumn("prix_unitaire_ht", F.col("prix_unitaire_ht").cast(DecimalType(10, 2)))
    .withColumn("remise_ligne_pct", F.coalesce(F.col("remise_ligne_pct").cast(DecimalType(5, 2)), F.lit(0)))
    .withColumn("montant_ligne_ht", F.round(F.col("quantite") * F.col("prix_unitaire_ht") * (1 - F.col("remise_ligne_pct") / 100), 2))
    .filter((F.col("ligne_id").isNotNull()) & (F.col("quantite") > 0))
    .withColumn("_cleaned_ts", F.current_timestamp()).withColumn("_source_batch_id", F.col("_batch_id"))
    .select("ligne_id", "commande_id", "produit_id", "quantite", "prix_unitaire_ht",
            "remise_ligne_pct", "montant_ligne_ht", "_cleaned_ts", "_source_batch_id")
)
df_clean_l.write.format("delta").mode("overwrite").saveAsTable("lh_sales.silver.clean_sales_distributions")
print(f"✅ clean_sales_distributions : {df_clean_l.count()} lignes")
