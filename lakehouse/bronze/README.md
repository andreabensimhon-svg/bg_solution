# Bronze - Données brutes

Données ingérées telles quelles depuis les sources. Toutes les colonnes sont en `STRING` (sauf `_ingestion_ts`).

## Fichiers
- [`schema_bronze.sql`](schema_bronze.sql) : DDL complet de toutes les tables Bronze

## Tables
| Table | Source | Mode |
|-------|--------|------|
| `raw_clients` | BDD clients partagée | Overwrite |
| `raw_produits` | ERP - Référentiel | Overwrite |
| `raw_erp_finance` | ERP - Transactions | Append |
| `raw_erp_bilans` | ERP - Bilans comptables | Overwrite |
| `raw_erp_operations` | ERP - Ordres de fabrication | Append |
| `raw_supply_chain` | ERP - Supply chain | Append |
| `raw_process_sanitaire` | ERP - Contrôles sanitaires | Append |
| `raw_erp_sales` | ERP - Commandes ventes | Append |
| `raw_sales_lines` | ERP - Lignes de commande | Append |
| `raw_json_orders` | API JSON externe | Append |
| `raw_mails` | Office 365 | Append |
| `raw_supply_plans` | Documents extraits | Append |
| `raw_iot_images` | Capteurs IoT (images) | Append |
| `raw_iot_stream` | Eventstream capteurs IoT | Append |

## Colonnes techniques
Chaque table contient : `_ingestion_ts`, `_source_file`, `_batch_id`
