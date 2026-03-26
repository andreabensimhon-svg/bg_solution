# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold : Agrégations Opérations
# MAGIC Production, supply chain et process sanitaire

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../utils/helpers

# COMMAND ----------

# ========================
# FACT_PRODUCTION
# ========================

df_production = spark.read.table("silver.clean_operations_production")

df_gold_production = (
    df_production
    .withColumn("date_production", F.to_date("date_debut"))
    .withColumn("annee", F.year("date_production"))
    .withColumn("mois", F.month("date_production"))
    .withColumn("semaine", F.weekofyear("date_production"))
    .groupBy("annee", "mois", "semaine", "ligne_production", "produit_id")
    .agg(
        F.min("date_production").alias("date_production"),
        F.count("*").alias("nb_ordres"),
        F.sum("quantite_prevue").alias("quantite_prevue_total"),
        F.sum("quantite_produite").alias("quantite_produite_total"),
        F.round(F.avg("taux_rendement"), 2).alias("taux_rendement_moyen"),
        F.round(F.sum("duree_heures"), 2).alias("duree_totale_heures"),
        F.sum(F.when(F.col("statut") == "annule", 1).otherwise(0)).alias("nb_incidents")
    )
    .withColumn("_computed_ts", F.current_timestamp())
)

# Enrichissement avec catégorie produit
df_produits = spark.read.table("silver.clean_produits")
df_gold_production = (
    df_gold_production
    .join(df_produits.select("produit_id", "categorie"), "produit_id", "left")
    .withColumnRenamed("categorie", "categorie_produit")
    .select(
        "date_production", "annee", "mois", "semaine",
        "ligne_production", "produit_id", "categorie_produit",
        "nb_ordres", "quantite_prevue_total", "quantite_produite_total",
        "taux_rendement_moyen", "duree_totale_heures", "nb_incidents",
        "_computed_ts"
    )
)

df_gold_production.write.format("delta").mode("overwrite").saveAsTable("gold.fact_production")
print(f"✅ Gold fact_production : {df_gold_production.count()} lignes")

# COMMAND ----------

# ========================
# FACT_SUPPLY_CHAIN
# ========================

df_supply = spark.read.table("silver.clean_supply_chain")

df_gold_supply = (
    df_supply
    .withColumn("annee", F.year("date_commande"))
    .withColumn("mois", F.month("date_commande"))
    .withColumn("date_periode", F.make_date("annee", "mois", F.lit(1)))
    .groupBy("annee", "mois", "date_periode", "fournisseur_id", "produit_id")
    .agg(
        F.count("*").alias("nb_commandes"),
        F.sum("quantite").alias("quantite_totale"),
        F.sum(F.col("quantite") * F.col("prix_unitaire")).alias("montant_total"),
        F.round(F.avg(F.abs("retard_jours")), 1).alias("delai_moyen_jours"),
        F.round(
            F.sum(F.when(F.col("retard_jours") > 0, 1).otherwise(0)) / F.count("*") * 100, 2
        ).alias("taux_retard_pct"),
        F.round(
            F.sum(F.when(F.col("statut") == "livre", 1).otherwise(0)) / F.count("*") * 100, 2
        ).alias("taux_conformite_pct")
    )
    .withColumn("_computed_ts", F.current_timestamp())
)

# Enrichissement catégorie produit
df_gold_supply = (
    df_gold_supply
    .join(df_produits.select("produit_id", "categorie"), "produit_id", "left")
    .withColumnRenamed("categorie", "categorie_produit")
    .select(
        "date_periode", "annee", "mois",
        "fournisseur_id", "produit_id", "categorie_produit",
        "nb_commandes", "quantite_totale", "montant_total",
        "delai_moyen_jours", "taux_retard_pct", "taux_conformite_pct",
        "_computed_ts"
    )
)

df_gold_supply.write.format("delta").mode("overwrite").saveAsTable("gold.fact_supply_chain")
print(f"✅ Gold fact_supply_chain : {df_gold_supply.count()} lignes")

# COMMAND ----------

# ========================
# FACT_PROCESS_SANITAIRE
# ========================

df_sanitaire = spark.read.table("silver.clean_process_sanitaire")

df_gold_sanitaire = (
    df_sanitaire
    .withColumn("date_periode", F.to_date("date_controle"))
    .withColumn("annee", F.year("date_periode"))
    .withColumn("mois", F.month("date_periode"))
    .withColumn("date_periode", F.make_date("annee", "mois", F.lit(1)))
    .groupBy("annee", "mois", "date_periode", "ligne_production", "type_controle")
    .agg(
        F.count("*").alias("nb_controles"),
        F.sum(F.when(F.col("resultat") == "conforme", 1).otherwise(0)).alias("nb_conformes"),
        F.sum(F.when(F.col("resultat") == "non_conforme", 1).otherwise(0)).alias("nb_non_conformes"),
        F.sum(F.when(F.col("resultat") == "alerte", 1).otherwise(0)).alias("nb_alertes"),
        F.round(F.avg("mesure_valeur"), 4).alias("valeur_moyenne"),
        F.round(F.min("mesure_valeur"), 4).alias("valeur_min"),
        F.round(F.max("mesure_valeur"), 4).alias("valeur_max")
    )
    .withColumn("taux_conformite_pct", F.round(
        F.col("nb_conformes") / F.col("nb_controles") * 100, 2
    ))
    .withColumn("_computed_ts", F.current_timestamp())
    .select(
        "date_periode", "annee", "mois",
        "ligne_production", "type_controle",
        "nb_controles", "nb_conformes", "nb_non_conformes", "nb_alertes",
        "taux_conformite_pct",
        "valeur_moyenne", "valeur_min", "valeur_max",
        "_computed_ts"
    )
)

df_gold_sanitaire.write.format("delta").mode("overwrite").saveAsTable("gold.fact_process_sanitaire")
print(f"✅ Gold fact_process_sanitaire : {df_gold_sanitaire.count()} lignes")
