# Lakehouse - Architecture Medallion

Organisation en 3 couches : Bronze → Silver → Gold.

## Couches
| Couche | Tables | Schéma | Description |
|--------|--------|--------|-------------|
| **Bronze** | 14 tables | [`schema_bronze.sql`](bronze/schema_bronze.sql) | Données brutes (STRING), métadonnées ingestion |
| **Silver** | 11 tables | [`schema_silver.sql`](silver/schema_silver.sql) | Nettoyées, typées, dédup, enrichies |
| **Gold** | 13 tables | [`schema_gold.sql`](gold/schema_gold.sql) | Agrégations métier, KPIs, segmentations |

## Flux de données
```
Sources (ERP, BDD, API, IoT, Mails)
       │
       ▼
   ┌─────────┐
   │ BRONZE  │  ← Pipelines + Eventstream + Copy Jobs
   └────┬────┘
       │ Notebooks Bronze→Silver
       ▼
   ┌─────────┐
   │ SILVER  │  ← Dédup + Typage + Enrichissement
   └────┬────┘
       │ Notebooks Silver→Gold
       ▼
   ┌─────────┐
   │  GOLD   │  ← Agrégations + KPIs + ML
   └────┬────┘
       │
       ├─▶ Warehouse (vues OLAP)
       ├─▶ Power BI (semantic model)
       └─▶ API (endpoints REST)
```
