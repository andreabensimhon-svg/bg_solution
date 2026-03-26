# Pipelines d'ingestion

Pipelines Data Factory pour orchestrer les flux batch et les copy jobs.

## Pipelines
| Pipeline | Description | Activités |
|----------|-------------|----------|
| `pl_orchestrator_main.json` | Orchestrateur principal (exécute tout) | Execute les 4 pipelines + 4 notebooks Gold + refresh PBI |
| `pl_ingest_clients.json` | Clients + Produits | Copy BDD → Bronze + notebook Silver |
| `pl_ingest_erp_finance.json` | Finance | Copy transactions + bilans → Bronze + notebook Silver |
| `pl_ingest_erp_sales.json` | Ventes | Copy commandes + lignes → Bronze + notebook Silver |
| `pl_ingest_erp_operations.json` | Opérations | Copy production + supply + sanitaire → Bronze + notebook Silver |
