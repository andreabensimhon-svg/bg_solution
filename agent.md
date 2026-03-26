# BGSolutions - Journal de Conversation

## Session du 20 mars 2026

### Contexte
- **Workspace** : `c:\Users\abensimhon\ProjetDemos\BGSolutions`
- **État initial** : Dossier vide

### Actions réalisées

1. **Création du fichier `agent.md`**
   - Fichier créé pour sauvegarder l'historique de la conversation et les décisions prises durant le projet.

2. **Connexion au repo GitHub `bg_solution`**
   - Repo créé : https://github.com/andreabensimhon-svg/bg_solution
   - Compte GitHub : andreabensimhon-svg
   - Premier commit poussé sur `main`

3. **Génération de l'arborescence du projet**
   - Basée sur le cas d'étude : entreprise de vente de matériaux de pointe pour carrosserie
   - 4 entités métier : Finance, Opérations, Sales (B2B), IT
   - Architecture Medallion (Bronze/Silver/Gold) dans le Lakehouse
   - Composants Fabric : Pipeline, Notebook, Eventstream, API, Dataflow Gen2, Operational Agent, Power BI, Copy Job, CI/CD, Spark Job, Cosmos DB, SQL DB, Warehouse, Lakehouse, ML Model, RT Dashboard, Function, Variables

---

## Session du 26 mars 2026

### Actions réalisées

1. **Suppression de toutes les références clients**
   - Remplacement de "Renault", "PSA" par "grands comptes" dans 4 fichiers

2. **Génération complète de tous les fichiers du projet**

   #### Lakehouse (3 schémas SQL)
   - `lakehouse/bronze/schema_bronze.sql` : 14 tables brutes (STRING, métadonnées ingestion)
   - `lakehouse/silver/schema_silver.sql` : 11 tables nettoyées (typées, validées)
   - `lakehouse/gold/schema_gold.sql` : 13 tables métier (agrégées, partitionnées)

   #### Notebooks (11 fichiers PySpark)
   - `notebooks/bronze_to_silver/` : 5 notebooks (clients, finance, operations, sales, iot)
   - `notebooks/silver_to_gold/` : 4 notebooks (finance, operations, sales, iot)
   - `notebooks/utils/helpers.py` : Fonctions partagées (dédup, DQ, écriture Delta)
   - `notebooks/utils/generate_demo_data.py` : Générateur de données démo Phase 1.1

   #### Ingestion (9 fichiers)
   - `ingestion/pipelines/` : 5 pipelines JSON (orchestrateur + 4 ingestions métier)
   - `ingestion/connectors/connectors_config.json` : 6 connecteurs (ERP, BDD, API, Mail, LH, Cosmos)
   - `ingestion/copy_jobs/cj_erp_clients_full.json` : Copy job full clients
   - `ingestion/dataflow_gen2/` : 2 Dataflows Power Query M (mails, JSON orders)

   #### Power BI (3 fichiers)
   - `power_bi/semantic_models/bgsolutions_model.tmdl` : Modèle sémantique TMDL
   - `power_bi/semantic_models/mesures_dax.dax` : 25+ mesures DAX par domaine
   - `power_bi/reports/reports_definitions.json` : 6 rapports avec pages et visuels

   #### Eventstream & RT Dashboard
   - `eventstream/es_iot_capteurs.json` : Source → transformations → 3 destinations
   - `rt_dashboard/rtd_capteurs_iot.json` : 9 tiles KQL temps réel

   #### API & Functions
   - `api/openapi_spec.json` : Spec OpenAPI 3.0 avec 6 endpoints REST
   - `functions/fabric_functions.py` : 4 fonctions (alerte IoT, enrichissement, validation, orchestration)

   #### Infrastructure
   - `cosmos_db/cosmos_collections.json` : 3 containers avec documents exemples
   - `sql_db/schema_sqldb.sql` : 6 tables OLTP (users, audit, alertes, objectifs, seuils IoT)
   - `warehouse/warehouse_views.sql` : 11 vues OLAP réparties sur 4 schémas

   #### ML & Spark
   - `ml_models/ml_iot_anomaly_detection.py` : K-Means + distance clustering
   - `ml_models/ml_forecast_ventes.py` : Prophet pour prévision CA
   - `spark_jobs/sj_delta_maintenance.py` : OPTIMIZE + VACUUM + Z-ORDER

   #### Config & CI/CD
   - `config/settings.json` : Configuration complète (connexions, schedules, seuils, DQ)
   - `config/.env.template` : Template variables d'environnement
   - `cicd/deployment_config.json` : Pipeline Dev → Test → Prod + Git integration
   - `operational_agent/agent_config.json` : Agent monitoring avec tâches planifiées

3. **Mise à jour de tous les 29 READMEs** avec les fichiers générés

---

*Ce fichier sera mis à jour au fil de la conversation pour garder une trace de toutes les actions et décisions.*
