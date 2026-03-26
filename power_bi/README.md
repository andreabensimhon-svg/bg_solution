# Power BI - Rapports et Semantic Models

## Fichiers
- `semantic_models/bgsolutions_model.tmdl` : Modèle sémantique TMDL complet
- `semantic_models/mesures_dax.dax` : 25+ mesures DAX par domaine
- `reports/reports_definitions.json` : Définitions des 6 rapports

## Rapports

### Finance
| Rapport | Pages | Mesures clés |
|---------|-------|--------------|
| Marges & Forecast | Vue d'ensemble, Détail Marges | CA, Marge Brute %, Croissance, Écart Forecast |
| Bilans Comptables | Actif/Passif, Compte de Résultat | Montant, Variation N-1 % |

### Opérations
| Rapport | Pages | Mesures clés |
|---------|-------|--------------|
| Production & Supply | Production, Supply Chain | Taux Rendement, Délai Livraison, Taux Retard |
| Process Sanitaire | Conformité | Taux Conformité, Non Conformités, Alertes |

### Sales
| Rapport | Pages | Mesures clés |
|---------|-------|--------------|
| Ventes B2B | Vue d'ensemble, Analyse Clients | CA Net, Panier Moyen, Segmentation ABC |
| Grands Comptes | Suivi | CA vs Objectif, Atteinte % |

### IoT / Temps réel
- RT Dashboard capteurs (voir `rt_dashboard/`)
