# Databricks notebook source
# MAGIC %md
# MAGIC # Shared : Bronze → Silver - Clients & Produits
# MAGIC Nettoyage du référentiel commun (lh_shared)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# === CLIENTS ===
df_raw = spark.read.table("lh_shared.bronze.raw_clients")

df_clean = (
    df_raw
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("client_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1).drop("_row_num")
    .withColumn("client_id", F.col("client_id").cast(IntegerType()))
    .withColumn("date_creation", F.to_date("date_creation", "yyyy-MM-dd"))
    .withColumn("raison_sociale", F.trim(F.upper("raison_sociale")))
    .withColumn("type_client", F.lower(F.trim("type_client")))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("pays", F.coalesce(F.col("pays"), F.lit("France")))
    .withColumn("code_postal", F.lpad("code_postal", 5, "0"))
    .filter(F.col("client_id").isNotNull() & F.col("raison_sociale").isNotNull())
    .withColumn("is_active", F.lit(True))
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
    .select(
        "client_id", "raison_sociale", "type_client", "siret",
        "adresse", "code_postal", "ville", "pays",
        "telephone", "email", "contact_principal",
        "date_creation", "is_active", "_cleaned_ts", "_source_batch_id"
    )
)

df_clean.write.format("delta").mode("overwrite").saveAsTable("lh_shared.silver.clean_clients")
print(f"✅ clean_clients : {df_clean.count()} lignes")

# COMMAND ----------

# === PRODUITS ===
df_raw_prod = spark.read.table("lh_shared.bronze.raw_produits")

df_clean_prod = (
    df_raw_prod
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("produit_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1).drop("_row_num")
    .withColumn("produit_id", F.col("produit_id").cast(IntegerType()))
    .withColumn("prix_catalogue_ht", F.col("prix_catalogue_ht").cast("decimal(10,2)"))
    .withColumn("poids_kg", F.col("poids_kg").cast("decimal(8,3)"))
    .withColumn("fournisseur_id", F.col("fournisseur_id").cast(IntegerType()))
    .withColumn("categorie", F.lower(F.trim("categorie")))
    .withColumn("is_active", F.col("actif").cast("boolean"))
    .filter(F.col("produit_id").isNotNull() & F.col("nom_produit").isNotNull())
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
    .select(
        "produit_id", "nom_produit", "categorie", "sous_categorie",
        "reference", "prix_catalogue_ht", "unite_vente", "poids_kg",
        "fournisseur_id", "is_active", "_cleaned_ts", "_source_batch_id"
    )
)

df_clean_prod.write.format("delta").mode("overwrite").saveAsTable("lh_shared.silver.clean_produits")
print(f"✅ clean_produits : {df_clean_prod.count()} lignes")
