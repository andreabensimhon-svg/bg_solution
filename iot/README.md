# IoT (lh_iot)

Capteurs temps réel, détection d'anomalies, RT Dashboard.

## Lakehouse : `lh_iot`

| Couche | Tables |
|--------|--------|
| Bronze | `raw_iot_stream`, `raw_iot_images` |
| Silver | `clean_iot_measures` |
| Gold | `fact_iot_realtime`, `fact_iot_anomalies`, `fact_iot_anomalies_ml` |

## Composants Fabric
- **Eventstream** : `es_iot_capteurs` (Custom Endpoint → 3 destinations)
- **KQL Database** : `db_iot_realtime`
- **RT Dashboard** : `rtd_capteurs_iot` (7 tiles KQL, refresh 30s)
- **Notebooks** : `nb_bronze_to_silver_iot`, `nb_silver_to_gold_iot`
- **ML Model** : `ml_iot_anomaly_detection` (K-Means + distance clustering)

## Architecture Eventstream
```
Capteurs → Custom Endpoint → Filter → Lakehouse Bronze
                                   → KQL Database
                           → Aggregate 5min → RT Dashboard
```
