# Cosmos DB

Base NoSQL pour les données semi-structurées et les lectures à faible latence.

## Fichiers
- [`cosmos_collections.json`](cosmos_collections.json) : Schémas des 3 containers avec documents exemples

## Containers
| Container | Partition Key | TTL | Usage |
|-----------|--------------|-----|-------|
| `clients` | `/type_client` | -1 (permanent) | Profils clients enrichis + historique interactions |
| `iot_events` | `/capteur_id` | 24h | Événements IoT récents pour lecture rapide |
| `mail_metadata` | `/langue_detectee` | -1 (permanent) | Métadonnées emails parsés + tags extraits |

## Configuration
- **Throughput** : Autoscale, max 4000 RU/s
- **Indexation** : Consistante, index ciblés par container
