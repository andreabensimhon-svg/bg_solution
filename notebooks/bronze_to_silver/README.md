# Notebooks Bronze → Silver

Nettoyage, dédoublonnage, typage, validation et normalisation des données.

## Notebooks
| Notebook | Tables produites | Traitements clés |
|----------|-----------------|-------------------|
| `nb_bronze_to_silver_clients.py` | `clean_clients` | Dédup, upper, typage, validation |
| `nb_bronze_to_silver_finance.py` | `clean_finance_transactions`, `clean_bilans_comptables` | Dédup, typage décimal, recalcul TTC |
| `nb_bronze_to_silver_operations.py` | `clean_operations_production`, `clean_supply_chain`, `clean_process_sanitaire` | Calcul rendement, retard, conformité seuils |
| `nb_bronze_to_silver_sales.py` | `clean_sales_orders`, `clean_sales_distributions` | Calcul montant après remise, jointure lignes |
| `nb_bronze_to_silver_iot.py` | `clean_iot_measures` | Typage double, filtrage 30j, détection anomalies simples |
