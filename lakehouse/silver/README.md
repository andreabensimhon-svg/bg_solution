# Silver - Données nettoyées et conformes

Données validées, dédupliquées, typées et enrichies.

## Fichiers
- [`schema_silver.sql`](schema_silver.sql) : DDL complet de toutes les tables Silver

## Tables
| Table | Traitements | Notebook source |
|-------|-------------|----------------|
| `clean_clients` | Dédup, typage, normalisation | `nb_bronze_to_silver_clients` |
| `clean_produits` | Dédup, typage | `nb_bronze_to_silver_clients` |
| `clean_finance_transactions` | Dédup, typage, recalcul TTC | `nb_bronze_to_silver_finance` |
| `clean_bilans_comptables` | Dédup, typage | `nb_bronze_to_silver_finance` |
| `clean_operations_production` | Dédup, calcul rendement | `nb_bronze_to_silver_operations` |
| `clean_supply_chain` | Dédup, calcul retard | `nb_bronze_to_silver_operations` |
| `clean_process_sanitaire` | Dédup, calcul conformité seuils | `nb_bronze_to_silver_operations` |
| `clean_sales_orders` | Dédup, calcul remise | `nb_bronze_to_silver_sales` |
| `clean_sales_distributions` | Dédup, calcul montant ligne | `nb_bronze_to_silver_sales` |
| `clean_iot_measures` | Dédup, détection anomalies simples | `nb_bronze_to_silver_iot` |
| `clean_mails_parsed` | NLP, détection langue | Dataflow Gen2 |

## Colonnes techniques
Chaque table contient : `_cleaned_ts`, `_source_batch_id`
