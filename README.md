# BGSolutions - Plateforme Data Microsoft Fabric

## Cas d'étude
Petite entreprise spécialisée dans la vente de matériaux de pointe pour la carrosserie.

## Entités métier
| Entité | Périmètre |
|--------|-----------|
| **Finance** | Forecasts, calculs de marges, 3 bilans comptables |
| **Opérations** | Usine, supply chain, process sanitaire, production |
| **Sales (B2B)** | Vente & distribution, client & merch, gros client Renault |
| **IT** | Admin, intégration BDD outils tiers vers ERP, connecteurs |

## Sources de données
- **1 BDD clients partagée**
- **Structurées** : Tables relationnelles (ERP, CRM)
- **Semi-structurées** : JSON
- **Non-structurées** : Corps de mails, plans de supply, images capteurs IoT
- **Temps réel** : Capteurs IoT via Eventstream

## Usages
- Streaming (temps réel capteurs)
- Batch (systèmes legacy)
- API
- OLTP / OLAP

## Architecture Fabric
Pipeline | Notebook | Eventstream | API | Dataflow Gen2 | Operational Agent | Power BI | Copy Job | CI/CD | Spark Job Definition | Cosmos DB | SQL DB | Warehouse | Lakehouse | ML Model | RT Dashboard | Function | Variable
