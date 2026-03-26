# Functions

Fabric Functions pour les traitements event-driven.

## Fichiers
- [`fabric_functions.py`](fabric_functions.py) : 4 fonctions métier

## Fonctions
| Fonction | Trigger | Description |
|----------|---------|-------------|
| `fn_alert_iot_anomaly` | Event Hub | Alerte sur anomalie IoT critique + log Cosmos DB |
| `fn_enrich_client` | HTTP | Enrichissement client temps réel (score, segment) |
| `fn_validate_json_order` | HTTP | Validation commande JSON avant ingestion |
| `fn_daily_refresh_orchestrator` | Timer | Orchestration du refresh quotidien complet |
