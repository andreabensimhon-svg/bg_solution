# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver : Nettoyage des données Clients
# MAGIC Dédoublonnage, typage, validation et normalisation

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType, BooleanType

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# Lecture des données Bronze
df_raw = spark.read.table("bronze.raw_clients")

# COMMAND ----------

# Nettoyage et transformation
df_clean = (
    df_raw
    # Dédoublonnage sur client_id (garder le plus récent)
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("client_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    # Typage
    .withColumn("client_id", F.col("client_id").cast(IntegerType()))
    .withColumn("date_creation", F.to_date("date_creation", "yyyy-MM-dd"))
    # Normalisation
    .withColumn("raison_sociale", F.trim(F.upper("raison_sociale")))
    .withColumn("type_client", F.lower(F.trim("type_client")))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("pays", F.coalesce(F.col("pays"), F.lit("France")))
    .withColumn("code_postal", F.lpad("code_postal", 5, "0"))
    # Validation
    .filter(F.col("client_id").isNotNull())
    .filter(F.col("raison_sociale").isNotNull())
    # Marqueur d'activité
    .withColumn("is_active", F.lit(True))
    # Métadonnées
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
    # Sélection colonnes Silver
    .select(
        "client_id", "raison_sociale", "type_client", "siret",
        "adresse", "code_postal", "ville", "pays",
        "telephone", "email", "contact_principal",
        "date_creation", "is_active",
        "_cleaned_ts", "_source_batch_id"
    )
)

# COMMAND ----------

# Écriture en mode merge (upsert) dans Silver
df_clean.write.format("delta").mode("overwrite").saveAsTable("silver.clean_clients")

print(f"✅ Silver clean_clients : {df_clean.count()} lignes écrites")
