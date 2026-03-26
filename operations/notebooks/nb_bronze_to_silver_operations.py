# Databricks notebook source
# MAGIC %md
# MAGIC # Operations : Bronze → Silver
# MAGIC Production, supply chain et process sanitaire (lh_operations)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType

# COMMAND ----------

# === PRODUCTION ===
df = spark.read.table("lh_operations.bronze.raw_erp_operations")
df_clean = (
    df.withColumn("_rn", F.row_number().over(Window.partitionBy("ordre_fabrication_id").orderBy(F.col("_ingestion_ts").desc())))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("ordre_fabrication_id", F.col("ordre_fabrication_id").cast(IntegerType()))
    .withColumn("produit_id", F.col("produit_id").cast(IntegerType()))
    .withColumn("quantite_prevue", F.col("quantite_prevue").cast(IntegerType()))
    .withColumn("quantite_produite", F.col("quantite_produite").cast(IntegerType()))
    .withColumn("date_debut", F.to_timestamp("date_debut"))
    .withColumn("date_fin", F.to_timestamp("date_fin"))
    .withColumn("duree_heures", F.round((F.unix_timestamp("date_fin") - F.unix_timestamp("date_debut")) / 3600, 2))
    .withColumn("taux_rendement", F.round(
        F.when(F.col("quantite_prevue") > 0, (F.col("quantite_produite") / F.col("quantite_prevue")) * 100).otherwise(None), 2))
    .withColumn("statut", F.lower(F.trim("statut")))
    .filter(F.col("ordre_fabrication_id").isNotNull() & F.col("statut").isNotNull())
    .withColumn("_cleaned_ts", F.current_timestamp()).withColumn("_source_batch_id", F.col("_batch_id"))
    .select("ordre_fabrication_id", "produit_id", "ligne_production", "quantite_prevue", "quantite_produite",
            "date_debut", "date_fin", "duree_heures", "statut", "operateur", "taux_rendement",
            "_cleaned_ts", "_source_batch_id")
)
df_clean.write.format("delta").mode("overwrite").saveAsTable("lh_operations.silver.clean_operations_production")
print(f"✅ clean_operations_production : {df_clean.count()} lignes")

# COMMAND ----------

# === SUPPLY CHAIN ===
df_s = spark.read.table("lh_operations.bronze.raw_supply_chain")
df_clean_s = (
    df_s.withColumn("_rn", F.row_number().over(Window.partitionBy("commande_fournisseur_id").orderBy(F.col("_ingestion_ts").desc())))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("commande_fournisseur_id", F.col("commande_fournisseur_id").cast(IntegerType()))
    .withColumn("fournisseur_id", F.col("fournisseur_id").cast(IntegerType()))
    .withColumn("produit_id", F.col("produit_id").cast(IntegerType()))
    .withColumn("quantite", F.col("quantite").cast(IntegerType()))
    .withColumn("prix_unitaire", F.col("prix_unitaire").cast(DecimalType(10, 2)))
    .withColumn("date_commande", F.to_date("date_commande", "yyyy-MM-dd"))
    .withColumn("date_livraison_prevue", F.to_date("date_livraison_prevue", "yyyy-MM-dd"))
    .withColumn("date_livraison_reelle", F.to_date("date_livraison_reelle", "yyyy-MM-dd"))
    .withColumn("retard_jours", F.datediff("date_livraison_reelle", "date_livraison_prevue"))
    .withColumn("statut", F.lower(F.trim("statut")))
    .filter(F.col("commande_fournisseur_id").isNotNull() & F.col("date_commande").isNotNull())
    .withColumn("_cleaned_ts", F.current_timestamp()).withColumn("_source_batch_id", F.col("_batch_id"))
    .select("commande_fournisseur_id", "fournisseur_id", "produit_id", "quantite", "prix_unitaire",
            "date_commande", "date_livraison_prevue", "date_livraison_reelle", "retard_jours",
            "statut", "entrepot_destination", "_cleaned_ts", "_source_batch_id")
)
df_clean_s.write.format("delta").mode("overwrite").saveAsTable("lh_operations.silver.clean_supply_chain")
print(f"✅ clean_supply_chain : {df_clean_s.count()} lignes")

# COMMAND ----------

# === PROCESS SANITAIRE ===
df_p = spark.read.table("lh_operations.bronze.raw_process_sanitaire")
df_clean_p = (
    df_p.withColumn("_rn", F.row_number().over(Window.partitionBy("controle_id").orderBy(F.col("_ingestion_ts").desc())))
    .filter(F.col("_rn") == 1).drop("_rn")
    .withColumn("controle_id", F.col("controle_id").cast(IntegerType()))
    .withColumn("date_controle", F.to_timestamp("date_controle"))
    .withColumn("mesure_valeur", F.col("mesure_valeur").cast(DecimalType(10, 4)))
    .withColumn("seuil_min", F.col("seuil_min").cast(DecimalType(10, 4)))
    .withColumn("seuil_max", F.col("seuil_max").cast(DecimalType(10, 4)))
    .withColumn("is_dans_seuils", (F.col("mesure_valeur") >= F.col("seuil_min")) & (F.col("mesure_valeur") <= F.col("seuil_max")))
    .withColumn("type_controle", F.lower(F.trim("type_controle")))
    .withColumn("resultat", F.lower(F.trim("resultat")))
    .filter(F.col("controle_id").isNotNull() & F.col("date_controle").isNotNull())
    .withColumn("_cleaned_ts", F.current_timestamp()).withColumn("_source_batch_id", F.col("_batch_id"))
    .select("controle_id", "ligne_production", "type_controle", "date_controle", "resultat",
            "mesure_valeur", "mesure_unite", "seuil_min", "seuil_max", "is_dans_seuils",
            "operateur", "commentaire", "_cleaned_ts", "_source_batch_id")
)
df_clean_p.write.format("delta").mode("overwrite").saveAsTable("lh_operations.silver.clean_process_sanitaire")
print(f"✅ clean_process_sanitaire : {df_clean_p.count()} lignes")
