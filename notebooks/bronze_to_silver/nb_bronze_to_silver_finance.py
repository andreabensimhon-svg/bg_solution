# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver : Nettoyage des données Finance
# MAGIC Transactions financières et bilans comptables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType, DateType

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# ========================
# TRANSACTIONS FINANCIÈRES
# ========================

df_raw_finance = spark.read.table("bronze.raw_erp_finance")

df_clean_finance = (
    df_raw_finance
    # Dédoublonnage
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("transaction_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    # Typage
    .withColumn("transaction_id", F.col("transaction_id").cast(IntegerType()))
    .withColumn("date_transaction", F.to_date("date_transaction", "yyyy-MM-dd"))
    .withColumn("client_id", F.col("client_id").cast(IntegerType()))
    .withColumn("montant_ht", F.col("montant_ht").cast(DecimalType(12, 2)))
    .withColumn("montant_tva", F.col("montant_tva").cast(DecimalType(12, 2)))
    .withColumn("montant_ttc", F.col("montant_ttc").cast(DecimalType(12, 2)))
    # Normalisation
    .withColumn("type_transaction", F.lower(F.trim("type_transaction")))
    .withColumn("devise", F.coalesce(F.col("devise"), F.lit("EUR")))
    .withColumn("statut", F.lower(F.trim("statut")))
    # Recalcul TTC si manquant
    .withColumn("montant_ttc", F.coalesce(
        F.col("montant_ttc"),
        F.col("montant_ht") + F.coalesce(F.col("montant_tva"), F.lit(0))
    ))
    # Validation
    .filter(F.col("transaction_id").isNotNull())
    .filter(F.col("date_transaction").isNotNull())
    .filter(F.col("montant_ht").isNotNull())
    # Métadonnées
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
    .select(
        "transaction_id", "date_transaction", "type_transaction",
        "client_id", "montant_ht", "montant_tva", "montant_ttc",
        "devise", "compte_comptable", "centre_cout",
        "description", "statut",
        "_cleaned_ts", "_source_batch_id"
    )
)

df_clean_finance.write.format("delta").mode("overwrite").saveAsTable("silver.clean_finance_transactions")
print(f"✅ Silver clean_finance_transactions : {df_clean_finance.count()} lignes")

# COMMAND ----------

# ========================
# BILANS COMPTABLES
# ========================

df_raw_bilans = spark.read.table("bronze.raw_erp_bilans")

df_clean_bilans = (
    df_raw_bilans
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("bilan_id").orderBy(F.col("_ingestion_ts").desc())
    ))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    # Typage
    .withColumn("bilan_id", F.col("bilan_id").cast(IntegerType()))
    .withColumn("annee_fiscale", F.col("annee_fiscale").cast(IntegerType()))
    .withColumn("montant", F.col("montant").cast(DecimalType(15, 2)))
    .withColumn("date_cloture", F.to_date("date_cloture", "yyyy-MM-dd"))
    # Normalisation
    .withColumn("type_bilan", F.lower(F.trim("type_bilan")))
    .withColumn("devise", F.coalesce(F.col("devise"), F.lit("EUR")))
    # Validation
    .filter(F.col("bilan_id").isNotNull())
    .filter(F.col("annee_fiscale").isNotNull())
    .filter(F.col("montant").isNotNull())
    # Métadonnées
    .withColumn("_cleaned_ts", F.current_timestamp())
    .withColumn("_source_batch_id", F.col("_batch_id"))
    .select(
        "bilan_id", "annee_fiscale", "type_bilan",
        "poste_comptable", "libelle", "montant",
        "devise", "date_cloture",
        "_cleaned_ts", "_source_batch_id"
    )
)

df_clean_bilans.write.format("delta").mode("overwrite").saveAsTable("silver.clean_bilans_comptables")
print(f"✅ Silver clean_bilans_comptables : {df_clean_bilans.count()} lignes")
