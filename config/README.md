# Variables et configuration

Variables d'environnement et paramètres partagés entre les composants.

## Fichiers
- [`settings.json`](settings.json) : Configuration complète du projet (connexions, schedules, seuils IoT, DQ)
- [`.env.template`](.env.template) : Template des variables d'environnement

## Sections
| Section | Contenu |
|---------|--------|
| `fabric` | IDs workspace, lakehouse, warehouse |
| `connections` | Serveurs ERP, BDD clients, Cosmos, API |
| `key_vault` | Nom du Key Vault + liste des secrets |
| `schedules` | Crons : orchestrateur (3h), maintenance (dim 1h), ML (6h), forecast (1er mois) |
| `data_quality` | Seuils DQ : nulls, doublons, fraîcheur par couche |
| `iot` | Seuils capteurs par type (warning/critical) |
