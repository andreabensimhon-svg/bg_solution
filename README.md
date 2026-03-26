# BGSolutions - Plateforme Data Microsoft Fabric

## Cas d'étude
Petite entreprise spécialisée dans la vente de matériaux de pointe pour la carrosserie.

## Architecture Domain-Driven

```
                     ┌──────────┐
                     │ lh_shared│  Référentiel commun
                     │ (clients,│  (exposé via Shortcuts)
                     │ produits)│
                     └────┬─────┘
                          │ Shortcuts ↓
       ┌──────────┬───────┼───────┬──────────┐
       ▼          ▼       ▼       ▼          ▼
  ┌─────────┐┌──────────┐┌────────┐┌─────────┐
  │lh_finance││lh_operat.││lh_sales││  lh_iot │
  │ B/S/G   ││  B/S/G   ││ B/S/G  ││  B/S/G  │
  └─────────┘└──────────┘└────────┘└─────────┘
       │          │          │          │
       ▼          ▼          ▼          ▼
  Pipeline    Pipeline   Pipeline   Eventstream
  Sem.Model   Sem.Model  Sem.Model  RT Dashboard
  2 Reports   2 Reports  2 Reports  ML Model
```

## Entités métier
| Entité | Périmètre | Lakehouse |
|--------|-----------|-----------|
| **Shared** | Clients, produits (référentiel) | `lh_shared` |
| **Finance** | Transactions, bilans, marges, forecast | `lh_finance` |
| **Opérations** | Production, supply chain, process sanitaire | `lh_operations` |
| **Sales (B2B)** | Commandes, distributions, grands comptes | `lh_sales` |
| **IoT** | Capteurs temps réel, anomalies | `lh_iot` |

## Sources de données
- **1 BDD clients partagée** → `lh_shared`
- **Structurées** : Tables ERP → domaines Finance/Operations/Sales
- **Semi-structurées** : JSON → `lh_sales`
- **Non-structurées** : Mails, images capteurs → `lh_sales`, `lh_iot`
- **Temps réel** : Capteurs IoT via Eventstream → `lh_iot`

## Arborescence

```
BGSolutions/
├── shared/            # Référentiel commun (lh_shared)
│   ├── lakehouse/     # Bronze/Silver/Gold schemas
│   ├── notebooks/     # 2 NB + utils (helpers, demo data)
│   ├── copy_jobs/     # cj_clients_full
│   └── connectors/    # 6 connecteurs + shortcuts config
├── finance/           # Domaine Finance (lh_finance)
│   ├── lakehouse/     # Bronze/Silver/Gold schemas
│   ├── notebooks/     # 2 NB (B→S, S→G)
│   ├── pipelines/     # pl_finance
│   ├── semantic_model/# SM_Finance + DAX
│   └── reports/       # 2 rapports (Marges, Bilans)
├── operations/        # Domaine Opérations (lh_operations)
│   ├── lakehouse/     # Bronze/Silver/Gold schemas
│   ├── notebooks/     # 2 NB
│   ├── pipelines/     # pl_operations
│   ├── semantic_model/# SM_Operations + DAX
│   └── reports/       # 2 rapports (Production, Sanitaire)
├── sales/             # Domaine Sales (lh_sales)
│   ├── lakehouse/     # Bronze/Silver/Gold schemas
│   ├── notebooks/     # 2 NB
│   ├── pipelines/     # pl_sales
│   ├── semantic_model/# SM_Sales + DAX
│   ├── reports/       # 2 rapports (Ventes, Grands comptes)
│   └── dataflow_gen2/ # 2 Dataflows (JSON, Mails)
├── iot/               # Domaine IoT (lh_iot)
│   ├── lakehouse/     # Bronze/Silver/Gold schemas
│   ├── notebooks/     # 2 NB
│   ├── eventstream/   # es_iot_capteurs
│   ├── rt_dashboard/  # rtd_capteurs_iot
│   └── ml_models/     # ml_iot_anomaly_detection
└── platform/          # Infrastructure cross-domaine
    ├── warehouse/     # Vues OLAP cross-Lakehouse
    ├── api/           # OpenAPI 3.0 spec
    ├── functions/     # Fabric Functions
    ├── cosmos_db/     # Collections NoSQL
    ├── sql_db/        # Tables OLTP
    ├── cicd/          # Dev→Test→Prod
    ├── operational_agent/ # Agent monitoring
    ├── spark_jobs/    # Delta maintenance
    ├── ml_models/     # Forecast ventes (cross-domaine)
    └── config/        # Settings + env template
```

## Plan de projet

📋 **[Voir le plan de projet complet →](PLAN.md)**
