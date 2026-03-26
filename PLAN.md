# BGSolutions - Plan de projet

## Vue d'ensemble

Plan de réalisation de la plateforme Data Microsoft Fabric pour une entreprise de vente de matériaux de pointe pour la carrosserie.

---

## Phase 1 - Fondations

> **Priorité : HAUTE** | Statut : A faire

| # | Tâche | Description | Statut |
|---|-------|-------------|--------|
| 1.1 | Générer les données de démo | Fake data réalistes pour chaque entité métier (clients, ventes, production, finance, IoT) | ⬜ A faire |
| 1.2 | Créer les notebooks Bronze → Silver → Gold | Transformations PySpark : nettoyage, conformité, agrégations métier | ⬜ A faire |
| 1.3 | Définir le schéma du Lakehouse | DDL des tables pour chaque couche (Bronze, Silver, Gold) par domaine | ⬜ A faire |

### Datasets de démo prévus

| Dataset | Exemples de données |
|---------|---------------------|
| **Clients** | Grands comptes, garages indépendants... |
| **Produits** | Matériaux carrosserie (primer, peinture, mastic, vernis...) |
| **Ventes** | Commandes B2B, bons de livraison, distributions |
| **Finance** | Factures, calculs de marges, bilans comptables |
| **Production** | Ordres de fabrication, supply chain, process sanitaire |
| **IoT** | Mesures capteurs (température, pression, humidité, images) |

---

## Phase 2 - Ingestion & Flux

> **Priorité : MOYENNE** | Statut : A faire

| # | Tâche | Description | Statut |
|---|-------|-------------|--------|
| 2.1 | Configurer les pipelines d'ingestion | JSON pipeline definitions pour les flux batch (ERP, BDD clients, fichiers) | ⬜ A faire |
| 2.2 | Mettre en place l'Eventstream | Flux temps réel depuis les capteurs IoT vers le Lakehouse | ⬜ A faire |
| 2.3 | Créer les Dataflow Gen2 | Transformations low-code pour les ingestions légères | ⬜ A faire |

---

## Phase 3 - Consommation & Reporting

> **Priorité : MOYENNE** | Statut : A faire

| # | Tâche | Description | Statut |
|---|-------|-------------|--------|
| 3.1 | Créer le modèle sémantique Power BI | Semantic model avec relations entre tables Gold | ⬜ A faire |
| 3.2 | Définir les mesures DAX | KPIs métier : marges, forecast, production, ventes grands comptes | ⬜ A faire |
| 3.3 | Préparer le RT Dashboard | Dashboard temps réel pour le suivi des capteurs IoT | ⬜ A faire |

### Rapports prévus

| Rapport | Entité | Contenu |
|---------|--------|---------|
| Marges & Forecast | Finance | Évolution marges, prévisions, alertes |
| Bilans comptables | Finance | 3 bilans comptables annuels |
| Production & Supply | Opérations | KPIs production, suivi supply chain |
| Process sanitaire | Opérations | Conformité et suivi sanitaire |
| Ventes B2B | Sales | Performance ventes, distribution |
| Grands comptes | Sales | Suivi dédié gros client |
| Capteurs IoT | IoT | RT Dashboard temps réel |

---

## Phase 4 - Intelligence & Automatisation

> **Priorité : BASSE** | Statut : A faire

| # | Tâche | Description | Statut |
|---|-------|-------------|--------|
| 4.1 | Notebook ML - Anomalies IoT | Détection d'anomalies sur les mesures capteurs | ⬜ A faire |
| 4.2 | Notebook ML - Forecast ventes | Prévision des ventes pour le forecast Finance | ⬜ A faire |
| 4.3 | Configuration CI/CD | Deployment pipelines Fabric : Dev → Test → Prod | ⬜ A faire |
| 4.4 | Documentation API | Endpoints pour exposer les données aux systèmes tiers | ⬜ A faire |

---

## Prochaine action

> **→ Phase 1.1 : Générer les données de démonstration**
>
> C'est la première brique qui rend le projet tangible et permet de tester chaque couche de l'architecture.
