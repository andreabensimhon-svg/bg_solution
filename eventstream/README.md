# Eventstream - Ingestion temps réel

Flux de données en temps réel depuis les capteurs IoT.

## Fichiers
- [`es_iot_capteurs.json`](es_iot_capteurs.json) : Configuration complète de l'Eventstream

## Architecture

```
Capteurs IoT (JSON)
       │
       ▼
  Custom Endpoint (4 partitions)
       │
       ├── [Filter] Mesures invalides supp
       │       │
       │       ├──▶ Lakehouse Bronze (raw_iot_stream)
       │       └──▶ KQL Database (iot_events_raw)
       │
       └── [Aggregate] Fenêtre 5 min
               │
               └──▶ RT Dashboard
```

## Transformations
1. **Parse timestamp** : Cast + ajout ingestion_time
2. **Filter** : `valeur IS NOT NULL AND qualite_signal > 50`
3. **Aggregate** : Tumbling window 5min (AVG, MIN, MAX, STDEV, COUNT)
