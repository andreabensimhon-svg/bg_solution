# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Job : Maintenance Delta Tables (tous domaines)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

LAKEHOUSES = ["lh_shared", "lh_finance", "lh_operations", "lh_sales", "lh_iot"]
SCHEMAS = ["bronze", "silver", "gold"]
VACUUM_HOURS = 168  # 7 jours

results = []
for lh in LAKEHOUSES:
    for schema in SCHEMAS:
        try:
            tables = spark.sql(f"SHOW TABLES IN {lh}.{schema}").collect()
            for t in tables:
                table_name = f"{lh}.{schema}.{t.tableName}"
                try:
                    spark.sql(f"OPTIMIZE {table_name}")
                    results.append({"table": table_name, "action": "OPTIMIZE", "status": "OK"})
                except Exception as e:
                    results.append({"table": table_name, "action": "OPTIMIZE", "status": f"ERROR: {e}"})
                try:
                    spark.sql(f"VACUUM {table_name} RETAIN {VACUUM_HOURS} HOURS")
                    results.append({"table": table_name, "action": "VACUUM", "status": "OK"})
                except Exception as e:
                    results.append({"table": table_name, "action": "VACUUM", "status": f"ERROR: {e}"})
        except Exception:
            pass

# COMMAND ----------

zorder_config = {
    "lh_finance.gold.fact_marges": ["annee", "mois"],
    "lh_sales.gold.fact_ventes": ["annee", "mois", "client_id"],
    "lh_operations.gold.fact_production": ["annee", "ligne_production"],
    "lh_iot.gold.fact_iot_realtime": ["capteur_id", "type_mesure"],
    "lh_iot.silver.clean_iot_measures": ["capteur_id", "timestamp_capteur"]
}

for table, cols in zorder_config.items():
    try:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({', '.join(cols)})")
        results.append({"table": table, "action": f"ZORDER({', '.join(cols)})", "status": "OK"})
    except Exception as e:
        results.append({"table": table, "action": "ZORDER", "status": f"ERROR: {e}"})

# COMMAND ----------

df_report = spark.createDataFrame(results)
df_report.show(200, truncate=False)
errors = [r for r in results if "ERROR" in r["status"]]
print(f"\n📊 {len(results)} opérations | {len(results) - len(errors)} OK | {len(errors)} erreurs")
