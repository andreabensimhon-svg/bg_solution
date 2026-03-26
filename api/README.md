# API

Endpoints API pour exposer les données aux systèmes tiers.

## Fichiers
- [`openapi_spec.json`](openapi_spec.json) : Spécification OpenAPI 3.0 complète

## Endpoints
| Méthode | Path | Description | Auth |
|---------|------|-------------|------|
| GET | `/clients` | Liste des clients (filtre type, ABC, ville) | Bearer JWT |
| GET | `/clients/{id}` | Détail client avec métriques | Bearer JWT |
| GET | `/finance/kpis` | KPIs financiers (CA, marges, forecast) | Bearer JWT |
| GET | `/sales/orders` | Statut commandes (filtre client, date, statut) | Bearer JWT |
| GET | `/iot/realtime` | Mesures IoT temps réel (filtre capteur, type) | Bearer JWT |
| GET | `/iot/anomalies` | Anomalies IoT détectées (filtre sévérité) | Bearer JWT |

## Modèles de données
- `Client` / `ClientDetail` : Informations client + métriques
- `FinanceKPIs` : CA, marges, croissance, forecast
- `SalesOrder` : Commande avec statut
- `IoTMeasure` : Mesure capteur agrégée
