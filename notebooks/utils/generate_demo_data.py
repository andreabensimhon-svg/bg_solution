# Databricks notebook source
# MAGIC %md
# MAGIC # Génération des données de démonstration
# MAGIC Phase 1.1 - Fake data réalistes pour chaque entité métier

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

random.seed(42)
NUM_CLIENTS = 150
NUM_PRODUITS = 80
NUM_TRANSACTIONS = 5000
NUM_COMMANDES = 3000
NUM_ORDRES_FAB = 2000
NUM_SUPPLY = 1500
NUM_CONTROLES = 1000
NUM_IOT_EVENTS = 50000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)

def random_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clients

# COMMAND ----------

types_client = ["grand_compte", "garage_independant", "distributeur", "concessionnaire"]
villes = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille", "Strasbourg", "Nice", "Rennes"]

clients_data = []
for i in range(1, NUM_CLIENTS + 1):
    clients_data.append((
        str(i),
        f"Client_{i:04d}",
        random.choice(types_client),
        f"{random.randint(100,999)}{random.randint(100,999)}{random.randint(100,999)}{random.randint(10,99)}",
        f"{random.randint(1,200)} rue de la Réussite",
        f"{random.randint(10000,95999)}",
        random.choice(villes),
        "France",
        f"+33{random.randint(100000000,999999999)}",
        f"contact@client{i:04d}.fr",
        f"Resp. Achats {i}",
        random_date(datetime(2015,1,1), datetime(2023,6,30)).strftime("%Y-%m-%d"),
        datetime.now().isoformat(),
        "demo_gen",
        "batch_demo_001"
    ))

schema_clients = StructType([
    StructField("client_id", StringType()), StructField("raison_sociale", StringType()),
    StructField("type_client", StringType()), StructField("siret", StringType()),
    StructField("adresse", StringType()), StructField("code_postal", StringType()),
    StructField("ville", StringType()), StructField("pays", StringType()),
    StructField("telephone", StringType()), StructField("email", StringType()),
    StructField("contact_principal", StringType()), StructField("date_creation", StringType()),
    StructField("_ingestion_ts", StringType()), StructField("_source_file", StringType()),
    StructField("_batch_id", StringType())
])

df_clients = spark.createDataFrame(clients_data, schema_clients)
df_clients.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_clients")
print(f"✅ raw_clients : {NUM_CLIENTS} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produits

# COMMAND ----------

categories = {
    "primer": ["Primer époxy", "Primer polyuréthane", "Primer acrylique", "Primer zinc"],
    "peinture": ["Base mate", "Base nacrée", "Base métallisée", "Peinture haute temp"],
    "mastic": ["Mastic polyester", "Mastic fibre", "Mastic aluminium", "Mastic fin"],
    "vernis": ["Vernis UHS", "Vernis MS", "Vernis mat", "Vernis anti-rayure"],
    "accessoire": ["Dégraissant", "Durcisseur", "Diluant", "Papier abrasif", "Ruban masquage"]
}

produits_data = []
pid = 1
for cat, prods in categories.items():
    for p in prods:
        for variant in range(1, random.randint(2, 5)):
            produits_data.append((
                str(pid), f"{p} V{variant}", cat, p,
                f"REF-{cat[:3].upper()}-{pid:04d}",
                str(round(random.uniform(5, 250), 2)),
                random.choice(["litre", "kg", "unité"]),
                str(round(random.uniform(0.1, 25), 3)),
                str(random.randint(1, 20)),
                "true",
                datetime.now().isoformat(), "demo_gen", "batch_demo_001"
            ))
            pid += 1

schema_produits = StructType([
    StructField("produit_id", StringType()), StructField("nom_produit", StringType()),
    StructField("categorie", StringType()), StructField("sous_categorie", StringType()),
    StructField("reference", StringType()), StructField("prix_catalogue_ht", StringType()),
    StructField("unite_vente", StringType()), StructField("poids_kg", StringType()),
    StructField("fournisseur_id", StringType()), StructField("actif", StringType()),
    StructField("_ingestion_ts", StringType()), StructField("_source_file", StringType()),
    StructField("_batch_id", StringType())
])

df_produits = spark.createDataFrame(produits_data, schema_produits)
df_produits.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_produits")
print(f"✅ raw_produits : {len(produits_data)} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions Finance

# COMMAND ----------

types_tx = ["facture", "avoir", "paiement"]
statuts_tx = ["validee", "en_attente", "annulee"]

finance_data = []
for i in range(1, NUM_TRANSACTIONS + 1):
    montant_ht = round(random.uniform(100, 50000), 2)
    tva = round(montant_ht * 0.2, 2)
    finance_data.append((
        str(i), random_date().strftime("%Y-%m-%d"),
        random.choice(types_tx), str(random.randint(1, NUM_CLIENTS)),
        str(montant_ht), str(tva), str(round(montant_ht + tva, 2)),
        "EUR", f"411{random.randint(100,999)}", f"CC{random.randint(1,10):02d}",
        f"Transaction {i}", random.choice(statuts_tx),
        datetime.now().isoformat(), "demo_gen", "batch_demo_001"
    ))

schema_finance = StructType([
    StructField("transaction_id", StringType()), StructField("date_transaction", StringType()),
    StructField("type_transaction", StringType()), StructField("client_id", StringType()),
    StructField("montant_ht", StringType()), StructField("montant_tva", StringType()),
    StructField("montant_ttc", StringType()), StructField("devise", StringType()),
    StructField("compte_comptable", StringType()), StructField("centre_cout", StringType()),
    StructField("description", StringType()), StructField("statut", StringType()),
    StructField("_ingestion_ts", StringType()), StructField("_source_file", StringType()),
    StructField("_batch_id", StringType())
])

df_finance = spark.createDataFrame(finance_data, schema_finance)
df_finance.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_erp_finance")
print(f"✅ raw_erp_finance : {NUM_TRANSACTIONS} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## IoT Stream

# COMMAND ----------

capteurs = [f"CAPT-{z}-{i:03d}" for z in ["PROD", "STOCK", "LABO"] for i in range(1, 11)]
types_mesure = ["temperature", "pression", "humidite"]
lignes = ["L1", "L2", "L3", "L4"]
zones = ["zone_A", "zone_B", "zone_C"]

iot_data = []
for i in range(1, NUM_IOT_EVENTS + 1):
    type_m = random.choice(types_mesure)
    if type_m == "temperature":
        valeur = round(random.gauss(80, 15), 2)
        unite = "celsius"
    elif type_m == "pression":
        valeur = round(random.gauss(100, 20), 2)
        unite = "bar"
    else:
        valeur = round(random.gauss(55, 10), 2)
        unite = "pct"

    iot_data.append((
        str(i), random.choice(capteurs), type_m,
        str(valeur), unite,
        random_date(datetime(2025,1,1), datetime(2025,12,31)).isoformat(),
        random.choice(lignes), random.choice(zones),
        str(round(random.uniform(70, 100), 1)),
        datetime.now().isoformat(), "demo_gen", "batch_demo_001"
    ))

schema_iot = StructType([
    StructField("event_id", StringType()), StructField("capteur_id", StringType()),
    StructField("type_mesure", StringType()), StructField("valeur", StringType()),
    StructField("unite", StringType()), StructField("timestamp_capteur", StringType()),
    StructField("ligne_production", StringType()), StructField("zone", StringType()),
    StructField("qualite_signal", StringType()),
    StructField("_ingestion_ts", StringType()), StructField("_source_file", StringType()),
    StructField("_batch_id", StringType())
])

df_iot = spark.createDataFrame(iot_data, schema_iot)
df_iot.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_iot_stream")
print(f"✅ raw_iot_stream : {NUM_IOT_EVENTS} lignes")

# COMMAND ----------

print("🎉 Génération des données de démo terminée !")
