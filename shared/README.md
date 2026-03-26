# Shared - Référentiel commun (lh_shared)

Données partagées entre tous les domaines via **Lakehouse Shortcuts**.

## Lakehouse : `lh_shared`

| Couche | Tables |
|--------|--------|
| Bronze | `raw_clients`, `raw_produits` |
| Silver | `clean_clients`, `clean_produits` |
| Gold | `dim_clients` (segmentation ABC), `dim_products` (enrichi ventes) |

## Shortcuts exposés

| Source | Domaines cibles | Nom du Shortcut |
|--------|----------------|----------------|
| `gold.dim_clients` | finance, operations, sales | `shared_dim_clients` |
| `gold.dim_products` | finance, operations, sales | `shared_dim_products` |
| `silver.clean_clients` | sales | `shared_clean_clients` |
| `silver.clean_produits` | finance, operations, sales | `shared_clean_produits` |

## Composants Fabric
- **Copy Job** : `cj_clients_full` (daily 02h)
- **Notebooks** : `nb_bronze_to_silver_shared`, `nb_silver_to_gold_shared`
- **Connecteurs** : `connectors_config.json` (6 connecteurs + shortcuts)
- **Utils** : `helpers.py`, `generate_demo_data.py`
