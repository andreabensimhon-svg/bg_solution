# Finance (lh_finance)

Données financières : transactions, bilans comptables, marges, forecasts.

## Lakehouse : `lh_finance`

| Couche | Tables |
|--------|--------|
| Bronze | `raw_erp_finance`, `raw_erp_bilans` |
| Silver | `clean_finance_transactions`, `clean_bilans_comptables` |
| Gold | `fact_marges`, `fact_forecast`, `dim_bilans_comptables` |

## Shortcuts utilisés
- `shared_dim_clients` → segmentation client dans les marges
- `shared_dim_products` → enrichissement catégorie produit

## Composants Fabric
- **Pipeline** : `pl_finance` (ERP → Bronze → Silver → Gold → Refresh SM)
- **Notebooks** : `nb_bronze_to_silver_finance`, `nb_silver_to_gold_finance`
- **Semantic Model** : `SM_Finance` (3 tables fact + 1 dim shortcut)
- **Mesures DAX** : 9 mesures (CA, Marges, Croissance, Forecast)
- **Rapports** : Marges & Forecast (2 pages), Bilans Comptables (1 page)
