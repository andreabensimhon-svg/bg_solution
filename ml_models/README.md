# ML Models

Modèles de Machine Learning entraînés dans Fabric.

## Fichiers
| Modèle | Fichier | Algorithme | Output Gold |
|--------|---------|-----------|------------|
| Détection anomalies IoT | [`ml_iot_anomaly_detection.py`](ml_iot_anomaly_detection.py) | K-Means + distance IQR | `fact_iot_anomalies_ml` |
| Forecast ventes | [`ml_forecast_ventes.py`](ml_forecast_ventes.py) | Prophet (FB) | `fact_forecast` |

## Détails

### Détection anomalies IoT
- **Features** : température, pression, humidité (avg + std par fenêtre 5min)
- **Normalisation** : StandardScaler
- **Clustering** : K-Means (k=3)
- **Détection** : Distance au centre du cluster > P95
- **Sévérité** : info / warning (>P95) / critical (>1.5×P95)
- **Modèles sauvés** : `Files/ml_models/iot_anomaly_kmeans`, `iot_anomaly_scaler`

### Forecast ventes
- **Données** : CA net mensuel agrégé depuis `gold.fact_ventes`
- **Horizon** : 12 mois
- **Intervalles de confiance** : 80%
- **Modèle sauvé** : `Files/ml_models/forecast_prophet_model.pkl`
- **Schedule** : 1er de chaque mois à 5h
