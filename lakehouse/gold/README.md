# Gold - Données métier agrégées

Données prêtes à la consommation par les entités métier.

## Fichiers
- [`schema_gold.sql`](schema_gold.sql) : DDL complet de toutes les tables Gold

## Tables par domaine

### Finance
| Table | Description | Partitionnement | Notebook source |
|-------|-------------|-----------------|----------------|
| `fact_marges` | Marges par client/produit/période | annee, mois | `nb_silver_to_gold_finance` |
| `fact_forecast` | Prévisions CA avec intervalles confiance | - | `nb_silver_to_gold_finance` + `ml_forecast_ventes` |
| `dim_bilans_comptables` | 3 bilans annuels avec variation N-1 | - | `nb_silver_to_gold_finance` |

### Opérations
| Table | Description | Partitionnement | Notebook source |
|-------|-------------|-----------------|----------------|
| `fact_production` | KPIs production par ligne/semaine | annee, mois | `nb_silver_to_gold_operations` |
| `fact_supply_chain` | Performance fournisseurs | - | `nb_silver_to_gold_operations` |
| `fact_process_sanitaire` | Conformité sanitaire agrégée | - | `nb_silver_to_gold_operations` |

### Sales
| Table | Description | Partitionnement | Notebook source |
|-------|-------------|-----------------|----------------|
| `fact_ventes` | Ventes agrégées multi-dimension | annee, mois | `nb_silver_to_gold_sales` |
| `fact_clients` | Segmentation ABC + scoring fidélité | - | `nb_silver_to_gold_sales` |
| `dim_clients_grands_comptes` | Suivi dédié grands comptes + objectifs | - | `nb_silver_to_gold_sales` |
| `dim_products` | Référentiel enrichi avec métriques ventes | - | `nb_silver_to_gold_sales` |

### IoT
| Table | Description | Notebook source |
|-------|-------------|-----------------|
| `fact_iot_realtime` | Mesures agrégées par fenêtre 5 min | `nb_silver_to_gold_iot` |
| `fact_iot_anomalies` | Anomalies IQR (info/warning/critical) | `nb_silver_to_gold_iot` |
| `fact_iot_anomalies_ml` | Anomalies ML (K-Means) | `ml_iot_anomaly_detection` |
