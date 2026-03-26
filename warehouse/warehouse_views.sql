-- ============================================================
-- BGSolutions - Warehouse Schema
-- Vues et tables agrégées pour OLAP et reporting Power BI
-- ============================================================

-- =====================
-- SCHÉMA : finance
-- =====================

CREATE SCHEMA finance;

CREATE VIEW finance.v_marges_mensuelles AS
SELECT
    date_calcul,
    annee,
    mois,
    trimestre,
    categorie_produit,
    SUM(chiffre_affaires_ht)    AS ca_total_ht,
    SUM(cout_revient)           AS cout_revient_total,
    SUM(marge_brute)            AS marge_brute_totale,
    CASE WHEN SUM(chiffre_affaires_ht) > 0
         THEN ROUND(SUM(marge_brute) / SUM(chiffre_affaires_ht) * 100, 2)
         ELSE 0 END             AS marge_brute_pct,
    SUM(volume_vendu)           AS volume_total
FROM gold.fact_marges
GROUP BY date_calcul, annee, mois, trimestre, categorie_produit;

CREATE VIEW finance.v_forecast_vs_reel AS
SELECT
    f.annee_cible       AS annee,
    f.mois_cible        AS mois,
    f.categorie_produit,
    f.ca_prevu,
    f.ca_reel,
    f.ecart_pct,
    f.intervalle_confiance_bas,
    f.intervalle_confiance_haut,
    f.modele_utilise
FROM gold.fact_forecast f;

CREATE VIEW finance.v_bilans_comparatifs AS
SELECT
    annee_fiscale,
    type_bilan,
    poste_comptable,
    libelle,
    montant,
    montant_n_moins_1,
    variation_pct,
    devise
FROM gold.dim_bilans_comptables
ORDER BY annee_fiscale DESC, type_bilan, poste_comptable;

-- =====================
-- SCHÉMA : operations
-- =====================

CREATE SCHEMA operations;

CREATE VIEW operations.v_production_kpi AS
SELECT
    date_production,
    annee,
    mois,
    semaine,
    ligne_production,
    categorie_produit,
    SUM(nb_ordres)                  AS nb_ordres,
    SUM(quantite_prevue_total)      AS quantite_prevue,
    SUM(quantite_produite_total)    AS quantite_produite,
    AVG(taux_rendement_moyen)       AS taux_rendement_moyen,
    SUM(duree_totale_heures)        AS heures_totales,
    SUM(nb_incidents)               AS nb_incidents,
    CASE WHEN SUM(nb_ordres) > 0
         THEN ROUND(CAST(SUM(nb_incidents) AS FLOAT) / SUM(nb_ordres) * 100, 2)
         ELSE 0 END                 AS taux_incidents_pct
FROM gold.fact_production
GROUP BY date_production, annee, mois, semaine, ligne_production, categorie_produit;

CREATE VIEW operations.v_supply_chain_performance AS
SELECT
    date_periode,
    annee,
    mois,
    fournisseur_id,
    SUM(nb_commandes)           AS nb_commandes,
    SUM(quantite_totale)        AS quantite_totale,
    SUM(montant_total)          AS montant_total,
    AVG(delai_moyen_jours)      AS delai_moyen_jours,
    AVG(taux_retard_pct)        AS taux_retard_pct,
    AVG(taux_conformite_pct)    AS taux_conformite_pct
FROM gold.fact_supply_chain
GROUP BY date_periode, annee, mois, fournisseur_id;

CREATE VIEW operations.v_sanitaire_conformite AS
SELECT
    date_periode,
    annee,
    mois,
    ligne_production,
    type_controle,
    SUM(nb_controles)       AS nb_controles,
    SUM(nb_conformes)       AS nb_conformes,
    SUM(nb_non_conformes)   AS nb_non_conformes,
    SUM(nb_alertes)         AS nb_alertes,
    ROUND(
        CAST(SUM(nb_conformes) AS FLOAT) / NULLIF(SUM(nb_controles), 0) * 100, 2
    ) AS taux_conformite_pct
FROM gold.fact_process_sanitaire
GROUP BY date_periode, annee, mois, ligne_production, type_controle;

-- =====================
-- SCHÉMA : sales
-- =====================

CREATE SCHEMA sales;

CREATE VIEW sales.v_ventes_performance AS
SELECT
    v.date_vente,
    v.annee,
    v.mois,
    v.trimestre,
    v.canal_vente,
    v.type_client,
    v.categorie_produit,
    SUM(v.nb_commandes)     AS nb_commandes,
    SUM(v.quantite_totale)  AS quantite_totale,
    SUM(v.ca_brut_ht)       AS ca_brut_ht,
    SUM(v.ca_net_ht)        AS ca_net_ht,
    AVG(v.panier_moyen)     AS panier_moyen
FROM gold.fact_ventes v
GROUP BY v.date_vente, v.annee, v.mois, v.trimestre, v.canal_vente, v.type_client, v.categorie_produit;

CREATE VIEW sales.v_clients_ranking AS
SELECT
    c.client_id,
    c.raison_sociale,
    c.type_client,
    c.ville,
    c.pays,
    c.nb_commandes_total,
    c.ca_cumule_ht,
    c.ca_12_mois,
    c.panier_moyen,
    c.categorie_abc,
    c.score_fidelite,
    RANK() OVER (ORDER BY c.ca_cumule_ht DESC) AS rang_ca
FROM gold.fact_clients c;

CREATE VIEW sales.v_grands_comptes_suivi AS
SELECT
    gc.client_id,
    gc.raison_sociale,
    gc.annee,
    gc.mois,
    gc.ca_mensuel_ht,
    gc.ca_cumul_annee,
    gc.objectif_mensuel,
    gc.atteinte_objectif_pct,
    gc.nb_commandes
FROM gold.dim_clients_grands_comptes gc;

-- =====================
-- SCHÉMA : iot
-- =====================

CREATE SCHEMA iot;

CREATE VIEW iot.v_capteurs_resume AS
SELECT
    capteur_id,
    type_mesure,
    ligne_production,
    zone,
    COUNT(*)                AS nb_fenetres,
    AVG(valeur_moyenne)     AS valeur_moyenne_globale,
    MIN(valeur_min)         AS valeur_min_globale,
    MAX(valeur_max)         AS valeur_max_globale,
    AVG(ecart_type)         AS ecart_type_moyen,
    SUM(nb_mesures)         AS nb_mesures_total,
    SUM(CASE WHEN has_anomalie = 1 THEN 1 ELSE 0 END) AS nb_fenetres_anomalie
FROM gold.fact_iot_realtime
GROUP BY capteur_id, type_mesure, ligne_production, zone;

CREATE VIEW iot.v_anomalies_recentes AS
SELECT
    a.capteur_id,
    a.type_mesure,
    a.timestamp_detection,
    a.valeur_anomalie,
    a.valeur_attendue,
    a.ecart_pct,
    a.severite,
    a.ligne_production,
    a.zone,
    a.modele_detection,
    a.is_confirmee
FROM gold.fact_iot_anomalies a
ORDER BY a.timestamp_detection DESC;
