# Notebooks Silver → Gold

Agrégations métier, calculs KPIs et enrichissements.

## Notebooks
| Notebook | Tables produites | Logique métier |
|----------|-----------------|----------------|
| `nb_silver_to_gold_finance.py` | `fact_marges`, `dim_bilans_comptables`, `fact_forecast` | Marges brutes/nettes, variation N-1, moyenne mobile 3M |
| `nb_silver_to_gold_operations.py` | `fact_production`, `fact_supply_chain`, `fact_process_sanitaire` | KPIs production, taux retard/conformité |
| `nb_silver_to_gold_sales.py` | `fact_ventes`, `fact_clients`, `dim_clients_grands_comptes`, `dim_products` | Segmentation ABC, scoring fidélité, suivi objectifs |
| `nb_silver_to_gold_iot.py` | `fact_iot_realtime`, `fact_iot_anomalies` | Fenêtres 5min, détection IQR multi-sévérité |
