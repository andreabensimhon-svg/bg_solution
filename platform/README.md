# Platform - Infrastructure cross-domaine

Composants partagés entre tous les domaines.

## Composants

| Composant | Fichier | Description |
|-----------|---------|-------------|
| **Warehouse** | `warehouse/warehouse_views.sql` | 12 vues OLAP cross-Lakehouse (5 schémas) |
| **API** | `api/openapi_spec.json` | 6 endpoints REST (JWT) |
| **Functions** | `functions/fabric_functions.py` | 3 fonctions (alerte IoT, validation, orchestrateur) |
| **Cosmos DB** | `cosmos_db/cosmos_collections.json` | 2 containers (clients, iot_events) |
| **SQL DB** | `sql_db/schema_sqldb.sql` | Tables OLTP (users, audit, alertes, seuils) |
| **CI/CD** | `cicd/deployment_config.json` | Pipeline Dev→Test→Prod + Git |
| **Op. Agent** | `operational_agent/agent_config.json` | Monitoring + tâches planifiées |
| **Spark Job** | `spark_jobs/sj_delta_maintenance.py` | OPTIMIZE/VACUUM/ZORDER tous domaines |
| **ML Forecast** | `ml_models/ml_forecast_ventes.py` | Prophet (lh_sales → lh_finance) |
| **Config** | `config/settings.json`, `config/.env.template` | Configuration + env vars |
