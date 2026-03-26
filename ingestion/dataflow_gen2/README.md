# Dataflow Gen2

Flux de données low-code (Power Query M) pour transformations légères à l'ingestion.

## Fichiers
| Dataflow | Description | Source → Destination |
|----------|-------------|---------------------|
| `df_transform_mails.pq` | Parse mails : nettoyage corps, comptage PJ, détection langue | `raw_mails` → `clean_mails_parsed` |
| `df_parse_json_orders.pq` | Parse commandes JSON : expansion, typage, validation | `raw_json_orders` → table Silver |
