# BGSolutions - Plateforme Data Microsoft Fabric

## Cas d'étude
Petite entreprise spécialisée dans la vente de matériaux de pointe pour la carrosserie.

## Entités métier
| Entité | Périmètre |
|--------|-----------|
| **Finance** | Forecasts, calculs de marges, 3 bilans comptables |
| **Opérations** | Usine, supply chain, process sanitaire, production |
| **Sales (B2B)** | Vente & distribution, client & merch, grands comptes |
| **IT** | Admin, intégration BDD outils tiers vers ERP, connecteurs |

## Sources de données
- **1 BDD clients partagée**
- **Structurées** : Tables relationnelles (ERP, CRM)
- **Semi-structurées** : JSON
- **Non-structurées** : Corps de mails, plans de supply, images capteurs IoT
- **Temps réel** : Capteurs IoT via Eventstream

## Usages
- Streaming (temps réel capteurs)
- Batch (systèmes legacy)
- API
- OLTP / OLAP

## Architecture Fabric
Pipeline | Notebook | Eventstream | API | Dataflow Gen2 | Operational Agent | Power BI | Copy Job | CI/CD | Spark Job Definition | Cosmos DB | SQL DB | Warehouse | Lakehouse | ML Model | RT Dashboard | Function | Variable

## Arborescence des fichiers

```
BGSolutions/
├── lakehouse/
│   ├── bronze/schema_bronze.sql          # 14 tables brutes
│   ├── silver/schema_silver.sql          # 11 tables nettoyées
│   └── gold/schema_gold.sql             # 13 tables métier
├── notebooks/
│   ├── bronze_to_silver/                 # 5 notebooks nettoyage
│   ├── silver_to_gold/                   # 4 notebooks agrégation
│   └── utils/                            # helpers + générateur démo
├── ingestion/
│   ├── pipelines/                        # 5 pipelines JSON (orchestrateur + 4)
│   ├── connectors/connectors_config.json # 6 connecteurs
│   ├── copy_jobs/                        # Copy job clients
│   └── dataflow_gen2/                    # 2 Power Query M (mails, JSON)
├── power_bi/
│   ├── semantic_models/                  # TMDL + 25 mesures DAX
│   └── reports/                          # 6 rapports définitions
├── eventstream/es_iot_capteurs.json      # Eventstream IoT complet
├── rt_dashboard/rtd_capteurs_iot.json    # Dashboard temps réel KQL
├── api/openapi_spec.json                 # Spec OpenAPI 3.0 (6 endpoints)
├── functions/fabric_functions.py         # 4 fonctions event-driven
├── ml_models/                            # Anomalie IoT + Forecast ventes
├── spark_jobs/sj_delta_maintenance.py    # Maintenance Delta Tables
├── warehouse/warehouse_views.sql         # 11 vues OLAP (4 schémas)
├── cosmos_db/cosmos_collections.json     # 3 containers NoSQL
├── sql_db/schema_sqldb.sql              # 6 tables OLTP
├── config/                               # settings.json + .env.template
├── cicd/deployment_config.json           # Dev → Test → Prod
├── operational_agent/agent_config.json   # Agent monitoring
└── PLAN.md                               # Plan de projet 4 phases
```

## Plan de projet

📋 **[Voir le plan de projet complet →](PLAN.md)**

**Prochaine étape : Phase 1.1 - Générer les données de démonstration**
