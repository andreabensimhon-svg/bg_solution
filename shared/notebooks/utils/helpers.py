# Databricks notebook source
# MAGIC %md
# MAGIC # Helpers & Fonctions utilitaires partagées

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

def deduplicate(df: DataFrame, key_col: str, order_col: str = "_ingestion_ts") -> DataFrame:
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

def add_cleaning_metadata(df: DataFrame, batch_col: str = "_batch_id") -> DataFrame:
    return df.withColumn("_cleaned_ts", F.current_timestamp()).withColumn("_source_batch_id", F.col(batch_col))

def add_computed_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_computed_ts", F.current_timestamp())

def trim_and_lower(df: DataFrame, *cols: str) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, F.lower(F.trim(c)))
    return df

def check_not_null(df: DataFrame, *cols: str) -> DataFrame:
    for c in cols:
        df = df.filter(F.col(c).isNotNull())
    return df

def log_row_count(table_name: str, df: DataFrame) -> int:
    count = df.count()
    print(f"✅ {table_name} : {count} lignes")
    return count

def add_date_columns(df: DataFrame, date_col: str) -> DataFrame:
    return (df.withColumn("annee", F.year(date_col)).withColumn("mois", F.month(date_col))
              .withColumn("trimestre", F.quarter(date_col)).withColumn("semaine", F.weekofyear(date_col)))
