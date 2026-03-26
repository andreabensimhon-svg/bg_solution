# BGSolutions - Journal de Conversation

## Session du 20 mars 2026

### Contexte
- **Workspace** : `c:\Users\abensimhon\ProjetDemos\BGSolutions`
- **État initial** : Dossier vide

### Actions réalisées

1. **Création du fichier `agent.md`**
   - Fichier créé pour sauvegarder l'historique de la conversation et les décisions prises durant le projet.

2. **Connexion au repo GitHub `bg_solution`**
   - Repo créé : https://github.com/andreabensimhon-svg/bg_solution
   - Compte GitHub : andreabensimhon-svg
   - Premier commit poussé sur `main`

3. **Génération de l'arborescence du projet**
   - Basée sur le cas d'étude : entreprise de vente de matériaux de pointe pour carrosserie
   - 4 entités métier : Finance, Opérations, Sales (B2B), IT
   - Architecture Medallion (Bronze/Silver/Gold) dans le Lakehouse
   - Composants Fabric : Pipeline, Notebook, Eventstream, API, Dataflow Gen2, Operational Agent, Power BI, Copy Job, CI/CD, Spark Job, Cosmos DB, SQL DB, Warehouse, Lakehouse, ML Model, RT Dashboard, Function, Variables

---

## Session du 26 mars 2026 - Refactoring

### Actions réalisées

1. **Suppression des références clients** (Renault, PSA → grands comptes)

2. **Première génération** : tous les fichiers dans une structure flat par type

3. **Refactoring complet → Architecture Domain-Driven**
   - Suppression de l'ancienne structure (16 dossiers)
   - Nouvelle organisation par domaine métier : `shared/`, `finance/`, `operations/`, `sales/`, `iot/`, `platform/`
   - Chaque domaine a son propre Lakehouse, Pipeline, Notebooks, Semantic Model, Reports
   - Données partagées (clients, produits) via **Lakehouse Shortcuts**
   - Infrastructure cross-domaine dans `platform/`

   #### Domaines créés
   | Domaine | Lakehouse | Pipeline | SM | Reports | Spécificités |
   |---------|-----------|----------|-----|---------|-------------|
   | shared | lh_shared | - | - | - | Copy Job, Shortcuts, Utils |
   | finance | lh_finance | pl_finance | SM_Finance | 2 | Marges, Bilans, Forecast |
   | operations | lh_operations | pl_operations | SM_Operations | 2 | Production, Supply, Sanitaire |
   | sales | lh_sales | pl_sales | SM_Sales | 2 | Ventes, Grands Comptes, Dataflow Gen2 |
   | iot | lh_iot | - | - | - | Eventstream, KQL, RT Dashboard, ML |
   | platform | wh_bgsolutions | - | - | - | API, Functions, Cosmos, SQL, CI/CD, Spark |

---

*Ce fichier sera mis à jour au fil de la conversation pour garder une trace de toutes les actions et décisions.*
