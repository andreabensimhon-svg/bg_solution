# CI/CD - Déploiement continu

Configuration CI/CD pour le déploiement des artefacts Fabric.

## Fichiers
- [`deployment_config.json`](deployment_config.json) : Configuration déploiement + intégration Git

## Stratégie
- **Dev** → **Test** → **Prod**
- Git integration avec GitHub (`bg_solution`)
- Deployment pipelines Fabric

## Environnements
| Stage | Workspace | Lakehouse |
|-------|-----------|----------|
| Development | BGSolutions_Dev | lh_bgsolutions_dev |
| Test | BGSolutions_Test | lh_bgsolutions_test |
| Production | BGSolutions_Prod | lh_bgsolutions |

## Artefacts déployés
Lakehouse, Notebooks, Pipelines, Semantic Model, Reports, Eventstream, KQL Database, RT Dashboard, Warehouse, Spark Jobs, ML Models

## Checklist
- **Pré-déploiement** : Tests notebooks, validation connexions, paramètres pipeline, tests DQ
- **Post-déploiement** : Run pipeline complet, refresh PBI, vérif Eventstream, test API
