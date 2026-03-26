# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Job - Maintenance et optimisation Delta Tables
# MAGIC Job planifié pour l'optimisation des tables Delta du Lakehouse

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# Configuration
SCHEMAS = ["bronze", "silver", "gold"]
VACUUM_RETENTION_HOURS = 168  # 7 jours

results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. OPTIMIZE : Compaction des fichiers

# COMMAND ----------

for schema in SCHEMAS:
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
    for table in tables:
        table_name = f"{schema}.{table.tableName}"
        try:
            spark.sql(f"OPTIMIZE {table_name}")
            results.append({"table": table_name, "action": "OPTIMIZE", "status": "OK"})
            print(f"✅ OPTIMIZE {table_name}")
        except Exception as e:
            results.append({"table": table_name, "action": "OPTIMIZE", "status": f"ERROR: {str(e)}"})
            print(f"❌ OPTIMIZE {table_name} : {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. VACUUM : Nettoyage des anciens fichiers

# COMMAND ----------

for schema in SCHEMAS:
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
    for table in tables:
        table_name = f"{schema}.{table.tableName}"
        try:
            spark.sql(f"VACUUM {table_name} RETAIN {VACUUM_RETENTION_HOURS} HOURS")
            results.append({"table": table_name, "action": "VACUUM", "status": "OK"})
            print(f"✅ VACUUM {table_name}")
        except Exception as e:
            results.append({"table": table_name, "action": "VACUUM", "status": f"ERROR: {str(e)}"})
            print(f"❌ VACUUM {table_name} : {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Z-ORDER sur les tables à forte volumétrie

# COMMAND ----------

zorder_config = {
    "gold.fact_ventes": ["annee", "mois", "client_id"],
    "gold.fact_marges": ["annee", "mois"],
    "gold.fact_production": ["annee", "mois", "ligne_production"],
    "gold.fact_iot_realtime": ["capteur_id", "type_mesure"],
    "silver.clean_iot_measures": ["capteur_id", "timestamp_capteur"]
}

for table_name, zorder_cols in zorder_config.items():
    cols = ", ".join(zorder_cols)
    try:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
        results.append({"table": table_name, "action": f"ZORDER ({cols})", "status": "OK"})
        print(f"✅ ZORDER {table_name} BY ({cols})")
    except Exception as e:
        results.append({"table": table_name, "action": "ZORDER", "status": f"ERROR: {str(e)}"})
        print(f"❌ ZORDER {table_name} : {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Rapport final

# COMMAND ----------

df_report = spark.createDataFrame(results)
df_report.show(100, truncate=False)

errors = [r for r in results if "ERROR" in r["status"]]
print(f"\n📊 Résumé : {len(results)} opérations | {len(results) - len(errors)} OK | {len(errors)} erreurs")
