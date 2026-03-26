# Connecteurs

Connecteurs vers les systèmes tiers et l'ERP.

## Fichiers
- [`connectors_config.json`](connectors_config.json) : Configuration complète des connecteurs

## Connecteurs configurés
| Connecteur | Type | Description | Auth |
|-----------|------|-------------|------|
| `conn_erp_sql` | SqlServer | ERP (finance, opérations, ventes) | Service Principal / Key Vault |
| `conn_bdd_clients` | SqlServer | BDD clients partagée | Service Principal / Key Vault |
| `conn_json_api` | RestService | API JSON commandes tiers | OAuth2 |
| `conn_mail_exchange` | Office365 | Exchange pour extraction mails | Service Principal |
| `conn_lakehouse_bronze` | Lakehouse | Destination Lakehouse Bronze | Workspace identity |
| `conn_cosmos_db` | CosmosDb | Cosmos DB lectures faible latence | Key Vault |
