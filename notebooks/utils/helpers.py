# Databricks notebook source
# MAGIC %md
# MAGIC # Helpers & Fonctions utilitaires
# MAGIC Fonctions partagées entre tous les notebooks BGSolutions

# COMMAND ----------

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# ========================
# CONFIGURATION
# ========================

LAKEHOUSE_NAME = "lh_bgsolutions"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# ========================
# FONCTIONS DE NETTOYAGE
# ========================

def deduplicate(df: DataFrame, key_col: str, order_col: str = "_ingestion_ts") -> DataFrame:
    """Dédoublonne un DataFrame en gardant la ligne la plus récente par clé."""
    window = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def add_cleaning_metadata(df: DataFrame, batch_col: str = "_batch_id") -> DataFrame:
    """Ajoute les colonnes de métadonnées Silver."""
    return (
        df
        .withColumn("_cleaned_ts", F.current_timestamp())
        .withColumn("_source_batch_id", F.col(batch_col))
    )


def add_computed_metadata(df: DataFrame) -> DataFrame:
    """Ajoute les colonnes de métadonnées Gold."""
    return df.withColumn("_computed_ts", F.current_timestamp())


def trim_and_lower(df: DataFrame, *cols: str) -> DataFrame:
    """Applique trim + lower sur plusieurs colonnes."""
    for col in cols:
        df = df.withColumn(col, F.lower(F.trim(col)))
    return df


def trim_and_upper(df: DataFrame, *cols: str) -> DataFrame:
    """Applique trim + upper sur plusieurs colonnes."""
    for col in cols:
        df = df.withColumn(col, F.upper(F.trim(col)))
    return df

# ========================
# FONCTIONS DE VALIDATION
# ========================

def check_not_null(df: DataFrame, *cols: str) -> DataFrame:
    """Filtre les lignes où les colonnes spécifiées sont non null."""
    for col in cols:
        df = df.filter(F.col(col).isNotNull())
    return df


def log_row_count(table_name: str, df: DataFrame) -> None:
    """Affiche le nombre de lignes d'un DataFrame."""
    count = df.count()
    print(f"✅ {table_name} : {count} lignes")
    return count


def check_data_quality(df: DataFrame, table_name: str, key_col: str) -> dict:
    """Vérifie la qualité des données et retourne un rapport."""
    total = df.count()
    distinct_keys = df.select(key_col).distinct().count()
    null_keys = df.filter(F.col(key_col).isNull()).count()

    report = {
        "table": table_name,
        "total_rows": total,
        "distinct_keys": distinct_keys,
        "null_keys": null_keys,
        "has_duplicates": total != distinct_keys,
        "checked_at": datetime.now().isoformat()
    }

    print(f"📊 DQ Report - {table_name}:")
    print(f"   Total: {total} | Distinct keys: {distinct_keys} | Null keys: {null_keys}")
    if report["has_duplicates"]:
        print(f"   ⚠️ Doublons détectés : {total - distinct_keys}")

    return report

# ========================
# FONCTIONS D'ÉCRITURE
# ========================

def write_to_silver(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """Écrit un DataFrame dans la couche Silver."""
    full_name = f"{SILVER_SCHEMA}.{table_name}"
    df.write.format("delta").mode(mode).saveAsTable(full_name)
    log_row_count(full_name, df)


def write_to_gold(df: DataFrame, table_name: str, mode: str = "overwrite",
                  partition_cols: list = None) -> None:
    """Écrit un DataFrame dans la couche Gold."""
    full_name = f"{GOLD_SCHEMA}.{table_name}"
    writer = df.write.format("delta").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(full_name)
    log_row_count(full_name, df)

# ========================
# FONCTIONS DE DATES
# ========================

def add_date_columns(df: DataFrame, date_col: str) -> DataFrame:
    """Ajoute les colonnes annee, mois, trimestre, semaine à partir d'une colonne date."""
    return (
        df
        .withColumn("annee", F.year(date_col))
        .withColumn("mois", F.month(date_col))
        .withColumn("trimestre", F.quarter(date_col))
        .withColumn("semaine", F.weekofyear(date_col))
    )
