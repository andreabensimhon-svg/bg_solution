# Notebooks utilitaires

Fonctions partagées et outils de support.

## Fichiers
| Fichier | Description |
|---------|-------------|
| `helpers.py` | Fonctions partagées : dédup, nettoyage, validation, écriture Delta, métadonnées |
| `generate_demo_data.py` | Générateur de données de démo (Phase 1.1) : clients, produits, finance, IoT |

## Fonctions principales (`helpers.py`)
- `deduplicate()` : Dédoublonnage par clé avec tri temporel
- `add_cleaning_metadata()` / `add_computed_metadata()` : Métadonnées Silver/Gold
- `trim_and_lower()` / `trim_and_upper()` : Normalisation texte
- `check_not_null()` : Validation nullité
- `check_data_quality()` : Rapport DQ (doublons, nulls)
- `write_to_silver()` / `write_to_gold()` : Écriture Delta
- `add_date_columns()` : Colonnes temporelles (annee, mois, trimestre, semaine)
