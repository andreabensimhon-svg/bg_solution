# Operational Agent

Agent opérationnel pour l'automatisation et le monitoring de la plateforme.

## Fichiers
- [`agent_config.json`](agent_config.json) : Configuration complète de l'agent

## Capacités
| Capacité | Description |
|----------|-------------|
| **Data Query** | Interroger les données Gold et Warehouse (finance, operations, sales, iot) |
| **Pipeline Monitoring** | Vérifier le statut des 5 pipelines |
| **Alerting** | Gérer les alertes (IoT, finance, supply, sanitaire, système) |

## Tâches planifiées
| Tâche | Schedule | Action |
|-------|----------|--------|
| Santé pipelines | 8h quotidien | Vérifier échecs, créer alertes critiques |
| Anomalies IoT | 7h quotidien | Résumé anomalies 24h par sévérité |
| KPIs quotidiens | 9h lun-ven | CA, commandes, rendement, anomalies |

## Exemples de prompts
- "Quel est le CA total du mois dernier ?"
- "Y a-t-il des anomalies IoT critiques aujourd'hui ?"
- "Quel est le statut du dernier run du pipeline orchestrateur ?"
