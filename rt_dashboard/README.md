# RT Dashboard - Tableau de bord temps réel

Dashboard temps réel pour le suivi des capteurs IoT de production.

## Fichiers
- [`rtd_capteurs_iot.json`](rtd_capteurs_iot.json) : Définition complète du dashboard

## Tiles
| Tile | Type | Données | Rafraîchissement |
|------|------|---------|------------------|
| Température Moyenne | Gauge | Dernières 5 min | 30s |
| Pression Moyenne | Gauge | Dernières 5 min | 30s |
| Humidité Moyenne | Gauge | Dernières 5 min | 30s |
| Évolution temps réel | LineChart | 30 dernières min | 30s |
| Anomalies détectées | Card | 1 dernière heure | 30s |
| Événements/sec | Card | 1 dernière min | 30s |
| Capteurs actifs | Card | 5 dernières min | 30s |
| Heatmap par zone | HeatMap | 15 dernières min | 30s |
| Derniers événements | Table | Top 50 | 30s |

## Source de données
- KQL Database `db_iot_realtime` alimentée via Eventstream
