# SQL DB

Base SQL pour les besoins OLTP et les applications transactionnelles.

## Fichiers
- [`schema_sqldb.sql`](schema_sqldb.sql) : DDL complet avec données initiales

## Tables (schéma `app`)
| Table | Description | Index |
|-------|-------------|-------|
| `utilisateurs` | Utilisateurs de l'application (5 rôles, 4 entités) | - |
| `parametres` | Paramètres applicatifs clé/valeur typés | UQ sur `cle` |
| `audit_log` | Journal d'audit (actions, IP) | IX timestamp, IX utilisateur |
| `alertes` | Alertes et notifications (5 types, 3 sévérités) | IX type+sévérité, IX non lues |
| `objectifs_commerciaux` | Objectifs CA par client/mois | UQ client+période |
| `seuils_iot` | Seuils d'alerte par type capteur/mesure | UQ capteur+mesure |

## Données initiales
- 7 seuils IoT préconfigurés (PROD, STOCK, LABO)
