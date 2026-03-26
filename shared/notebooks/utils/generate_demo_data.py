# Databricks notebook source
# MAGIC %md
# MAGIC # Génération données de démo (Phase 1.1)
# MAGIC Alimente tous les Lakehouses domaines en données réalistes

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

random.seed(42)

def random_date(start=datetime(2023,1,1), end=datetime(2025,12,31)):
    return start + timedelta(days=random.randint(0, (end-start).days))

# COMMAND ----------

# MAGIC %md
# MAGIC ## lh_shared : Clients + Produits

# COMMAND ----------

types_client = ["grand_compte", "garage_independant", "distributeur", "concessionnaire"]
villes = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Nantes", "Lille", "Strasbourg", "Nice", "Rennes"]
NUM_CLIENTS = 150

clients = [(str(i), f"Client_{i:04d}", random.choice(types_client),
    f"{random.randint(100,999)}{random.randint(100,999)}{random.randint(100,999)}{random.randint(10,99)}",
    f"{random.randint(1,200)} rue de la Réussite", f"{random.randint(10000,95999)}",
    random.choice(villes), "France", f"+33{random.randint(100000000,999999999)}",
    f"contact@client{i:04d}.fr", f"Resp. Achats {i}",
    random_date(datetime(2015,1,1), datetime(2023,6,30)).strftime("%Y-%m-%d"),
    datetime.now().isoformat(), "demo_gen", "batch_demo_001")
    for i in range(1, NUM_CLIENTS + 1)]

schema_c = StructType([StructField(n, StringType()) for n in [
    "client_id","raison_sociale","type_client","siret","adresse","code_postal",
    "ville","pays","telephone","email","contact_principal","date_creation",
    "_ingestion_ts","_source_file","_batch_id"]])

spark.createDataFrame(clients, schema_c).write.format("delta").mode("overwrite").saveAsTable("lh_shared.bronze.raw_clients")
print(f"✅ lh_shared.bronze.raw_clients : {NUM_CLIENTS}")

# COMMAND ----------

categories = {"primer": ["Primer époxy","Primer PU","Primer acrylique"],
    "peinture": ["Base mate","Base nacrée","Base métallisée"],
    "mastic": ["Mastic polyester","Mastic fibre","Mastic alu"],
    "vernis": ["Vernis UHS","Vernis MS","Vernis mat"],
    "accessoire": ["Dégraissant","Durcisseur","Diluant","Papier abrasif"]}

prods = []
pid = 1
for cat, items in categories.items():
    for p in items:
        for v in range(1, random.randint(2,4)):
            prods.append((str(pid), f"{p} V{v}", cat, p, f"REF-{cat[:3].upper()}-{pid:04d}",
                str(round(random.uniform(5,250),2)), random.choice(["litre","kg","unité"]),
                str(round(random.uniform(0.1,25),3)), str(random.randint(1,20)), "true",
                datetime.now().isoformat(), "demo_gen", "batch_demo_001"))
            pid += 1

schema_p = StructType([StructField(n, StringType()) for n in [
    "produit_id","nom_produit","categorie","sous_categorie","reference",
    "prix_catalogue_ht","unite_vente","poids_kg","fournisseur_id","actif",
    "_ingestion_ts","_source_file","_batch_id"]])

spark.createDataFrame(prods, schema_p).write.format("delta").mode("overwrite").saveAsTable("lh_shared.bronze.raw_produits")
print(f"✅ lh_shared.bronze.raw_produits : {len(prods)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## lh_finance : Transactions + Bilans

# COMMAND ----------

NUM_TX = 5000
types_tx = ["facture","avoir","paiement"]
finance = [(str(i), random_date().strftime("%Y-%m-%d"), random.choice(types_tx),
    str(random.randint(1,NUM_CLIENTS)), str(round(random.uniform(100,50000),2)),
    str(round(random.uniform(100,50000)*0.2,2)), str(round(random.uniform(100,50000)*1.2,2)),
    "EUR", f"411{random.randint(100,999)}", f"CC{random.randint(1,10):02d}",
    f"Transaction {i}", random.choice(["validee","en_attente","annulee"]),
    datetime.now().isoformat(), "demo_gen", "batch_demo_001") for i in range(1, NUM_TX+1)]

schema_f = StructType([StructField(n, StringType()) for n in [
    "transaction_id","date_transaction","type_transaction","client_id",
    "montant_ht","montant_tva","montant_ttc","devise","compte_comptable",
    "centre_cout","description","statut","_ingestion_ts","_source_file","_batch_id"]])

spark.createDataFrame(finance, schema_f).write.format("delta").mode("overwrite").saveAsTable("lh_finance.bronze.raw_erp_finance")
print(f"✅ lh_finance.bronze.raw_erp_finance : {NUM_TX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## lh_iot : Stream capteurs

# COMMAND ----------

NUM_IOT = 50000
capteurs = [f"CAPT-{z}-{i:03d}" for z in ["PROD","STOCK","LABO"] for i in range(1,11)]
types_m = ["temperature","pression","humidite"]
lignes = ["L1","L2","L3","L4"]
zones = ["zone_A","zone_B","zone_C"]

iot = []
for i in range(1, NUM_IOT+1):
    tm = random.choice(types_m)
    val = round(random.gauss(80,15) if tm=="temperature" else random.gauss(100,20) if tm=="pression" else random.gauss(55,10), 2)
    iot.append((str(i), random.choice(capteurs), tm, str(val),
        "celsius" if tm=="temperature" else "bar" if tm=="pression" else "pct",
        random_date(datetime(2025,1,1), datetime(2025,12,31)).isoformat(),
        random.choice(lignes), random.choice(zones), str(round(random.uniform(70,100),1)),
        datetime.now().isoformat(), "demo_gen", "batch_demo_001"))

schema_i = StructType([StructField(n, StringType()) for n in [
    "event_id","capteur_id","type_mesure","valeur","unite","timestamp_capteur",
    "ligne_production","zone","qualite_signal","_ingestion_ts","_source_file","_batch_id"]])

spark.createDataFrame(iot, schema_i).write.format("delta").mode("overwrite").saveAsTable("lh_iot.bronze.raw_iot_stream")
print(f"✅ lh_iot.bronze.raw_iot_stream : {NUM_IOT}")

# COMMAND ----------

print("🎉 Données de démo générées dans tous les domaines !")
