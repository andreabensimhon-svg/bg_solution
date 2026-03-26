# Sales (lh_sales)

Ventes B2B, distributions, suivi grands comptes.

## Lakehouse : `lh_sales`

| Couche | Tables |
|--------|--------|
| Bronze | `raw_erp_sales`, `raw_sales_lines`, `raw_json_orders`, `raw_mails` |
| Silver | `clean_sales_orders`, `clean_sales_distributions`, `clean_mails_parsed` |
| Gold | `fact_ventes`, `dim_clients_grands_comptes` |

## Shortcuts utilisés
- `shared_dim_clients` → type client, segmentation ABC
- `shared_dim_products` → catégorie produit

## Composants Fabric
- **Pipeline** : `pl_sales` (ERP → Bronze → Silver → Gold → Refresh SM)
- **Notebooks** : `nb_bronze_to_silver_sales`, `nb_silver_to_gold_sales`
- **Dataflow Gen2** : `df_parse_json_orders.pq`, `df_transform_mails.pq`
- **Semantic Model** : `SM_Sales` (2 tables fact + 2 dim shortcuts)
- **Mesures DAX** : 8 mesures (CA, Commandes, Panier, Grands Comptes)
- **Rapports** : Ventes B2B (2 pages), Grands Comptes (1 page)
