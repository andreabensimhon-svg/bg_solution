# Semantic Models (Datasets) Power BI

## Fichiers
- [`bgsolutions_model.tmdl`](bgsolutions_model.tmdl) : Modèle tabulaire (TMDL) avec tables, colonnes et relations
- [`mesures_dax.dax`](mesures_dax.dax) : Mesures DAX organisées par domaine

## Tables du modèle
| Table | Type | Source Gold |
|-------|------|------------|
| `fact_marges` | Fait | `gold.fact_marges` |
| `fact_ventes` | Fait | `gold.fact_ventes` |
| `fact_production` | Fait | `gold.fact_production` |
| `fact_iot_realtime` | Fait | `gold.fact_iot_realtime` |
| `fact_clients` | Dimension | `gold.fact_clients` |
| `dim_products` | Dimension | `gold.dim_products` |
| `dim_bilans_comptables` | Dimension | `gold.dim_bilans_comptables` |

## Relations
- `fact_marges` → `fact_clients` (client_id)
- `fact_marges` → `dim_products` (produit_id)
- `fact_ventes` → `fact_clients` (client_id)
- `fact_ventes` → `dim_products` (produit_id)

## Mesures DAX (25+)
- **Finance** : CA Total, Marge Brute/Nette %, Croissance CA, Forecast, Écart
- **Sales** : Nb Commandes, Panier Moyen, CA par Client, Part Grands Comptes
- **Opérations** : Taux Rendement, Taux Incidents, Conformité Sanitaire, Délai Livraison
- **IoT** : Nb Mesures, Température Moy, Pression Moy, Anomalies
