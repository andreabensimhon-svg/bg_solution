# Opérations (lh_operations)

Production, supply chain, process sanitaire.

## Lakehouse : `lh_operations`

| Couche | Tables |
|--------|--------|
| Bronze | `raw_erp_operations`, `raw_supply_chain`, `raw_process_sanitaire` |
| Silver | `clean_operations_production`, `clean_supply_chain`, `clean_process_sanitaire` |
| Gold | `fact_production`, `fact_supply_chain`, `fact_process_sanitaire` |

## Shortcuts utilisés
- `shared_dim_products` → catégorie produit dans les KPIs

## Composants Fabric
- **Pipeline** : `pl_operations` (ERP → Bronze → Silver → Gold → Refresh SM)
- **Notebooks** : `nb_bronze_to_silver_operations`, `nb_silver_to_gold_operations`
- **Semantic Model** : `SM_Operations` (3 tables fact + 1 dim shortcut)
- **Mesures DAX** : 10 mesures (Rendement, Incidents, Retard, Conformité)
- **Rapports** : Production & Supply (2 pages), Process Sanitaire (1 page)
