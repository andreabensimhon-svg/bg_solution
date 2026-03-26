-- ============================================================
-- OPERATIONS - Gold : KPIs Opérations
-- Lakehouse : lh_operations
-- Shortcuts : shared_dim_products
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_production (
    production_id BIGINT GENERATED ALWAYS AS IDENTITY,
    date_production DATE NOT NULL, annee INT, mois INT, semaine INT,
    ligne_production STRING NOT NULL, produit_id INT, categorie_produit STRING,
    nb_ordres INT, quantite_prevue_total INT, quantite_produite_total INT,
    taux_rendement_moyen DECIMAL(5,2), duree_totale_heures DECIMAL(10,2),
    nb_incidents INT DEFAULT 0, _computed_ts TIMESTAMP
) USING DELTA PARTITIONED BY (annee, mois);

CREATE TABLE IF NOT EXISTS gold.fact_supply_chain (
    supply_id BIGINT GENERATED ALWAYS AS IDENTITY,
    date_periode DATE NOT NULL, annee INT, mois INT,
    fournisseur_id INT, produit_id INT, categorie_produit STRING,
    nb_commandes INT, quantite_totale INT, montant_total DECIMAL(15,2),
    delai_moyen_jours DECIMAL(5,1), taux_retard_pct DECIMAL(5,2),
    taux_conformite_pct DECIMAL(5,2), _computed_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.fact_process_sanitaire (
    sanitaire_id BIGINT GENERATED ALWAYS AS IDENTITY,
    date_periode DATE NOT NULL, annee INT, mois INT,
    ligne_production STRING NOT NULL, type_controle STRING NOT NULL,
    nb_controles INT, nb_conformes INT, nb_non_conformes INT, nb_alertes INT,
    taux_conformite_pct DECIMAL(5,2), valeur_moyenne DECIMAL(10,4),
    valeur_min DECIMAL(10,4), valeur_max DECIMAL(10,4), _computed_ts TIMESTAMP
) USING DELTA;
