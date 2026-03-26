# Spark Job Definitions

Jobs Spark planifiés pour les traitements batch lourds.

## Fichiers
- [`sj_delta_maintenance.py`](sj_delta_maintenance.py) : Maintenance et optimisation Delta Tables

## Détails

### Delta Maintenance (`sj_delta_maintenance`)
- **Schedule** : Chaque dimanche à 1h du matin
- **Actions** :
  1. **OPTIMIZE** : Compaction des petits fichiers sur toutes les tables (Bronze, Silver, Gold)
  2. **VACUUM** : Nettoyage des fichiers obsolètes (rétention 7 jours)
  3. **Z-ORDER** : Optimisation des index sur les tables à forte volumétrie
     - `fact_ventes` : annee, mois, client_id
     - `fact_marges` : annee, mois
     - `fact_production` : annee, mois, ligne_production
     - `fact_iot_realtime` : capteur_id, type_mesure
     - `clean_iot_measures` : capteur_id, timestamp_capteur
  4. **Rapport** : Synthèse des opérations avec succès/erreurs
