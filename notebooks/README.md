# Notebooks - Transformations Spark

Notebooks PySpark pour les transformations Bronze → Silver → Gold.

## Structure

```
notebooks/
├── bronze_to_silver/
│   ├── nb_bronze_to_silver_clients.py      # Clients + Produits
│   ├── nb_bronze_to_silver_finance.py      # Transactions + Bilans
│   ├── nb_bronze_to_silver_operations.py   # Production + Supply + Sanitaire
│   ├── nb_bronze_to_silver_sales.py        # Commandes + Lignes
│   └── nb_bronze_to_silver_iot.py          # Mesures capteurs
├── silver_to_gold/
│   ├── nb_silver_to_gold_finance.py        # Marges + Forecast + Bilans
│   ├── nb_silver_to_gold_operations.py     # Production + Supply + Sanitaire
│   ├── nb_silver_to_gold_sales.py          # Ventes + Clients + Grands comptes
│   └── nb_silver_to_gold_iot.py            # Agrégation + Anomalies
└── utils/
    ├── helpers.py                           # Fonctions partagées
    └── generate_demo_data.py               # Générateur données Phase 1.1
```

## Dépendance
Tous les notebooks utilisent `%run ../utils/helpers` pour les fonctions partagées.
