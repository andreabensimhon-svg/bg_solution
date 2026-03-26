# Warehouse - Couche OLAP

SQL Warehouse pour les requêtes analytiques et le reporting Power BI.

## Fichiers
- [`warehouse_views.sql`](warehouse_views.sql) : Vues SQL organisées par schéma

## Schémas et vues

### `finance`
| Vue | Source Gold | Description |
|----|------------|-------------|
| `v_marges_mensuelles` | `fact_marges` | Marges agrégées par catégorie/période |
| `v_forecast_vs_reel` | `fact_forecast` | Comparaison prévisions vs réalisé |
| `v_bilans_comparatifs` | `dim_bilans_comptables` | Bilans avec variation N-1 |

### `operations`
| Vue | Source Gold | Description |
|----|------------|-------------|
| `v_production_kpi` | `fact_production` | KPIs production avec taux incidents |
| `v_supply_chain_performance` | `fact_supply_chain` | Performance fournisseurs |
| `v_sanitaire_conformite` | `fact_process_sanitaire` | Taux conformité par ligne |

### `sales`
| Vue | Source Gold | Description |
|----|------------|-------------|
| `v_ventes_performance` | `fact_ventes` | Performance ventes multi-axes |
| `v_clients_ranking` | `fact_clients` | Classement clients par CA |
| `v_grands_comptes_suivi` | `dim_clients_grands_comptes` | Suivi dédié grands comptes |

### `iot`
| Vue | Source Gold | Description |
|----|------------|-------------|
| `v_capteurs_resume` | `fact_iot_realtime` | Résumé par capteur avec anomalies |
| `v_anomalies_recentes` | `fact_iot_anomalies` | Dernières anomalies détectées |
