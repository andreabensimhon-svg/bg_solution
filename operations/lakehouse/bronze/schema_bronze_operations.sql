-- ============================================================
-- OPERATIONS - Bronze : Données opérations brutes
-- Lakehouse : lh_operations
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.raw_erp_operations (
    ordre_fabrication_id STRING, produit_id STRING, ligne_production STRING,
    quantite_prevue STRING, quantite_produite STRING, date_debut STRING, date_fin STRING,
    statut STRING, operateur STRING, commentaire STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_supply_chain (
    commande_fournisseur_id STRING, fournisseur_id STRING, produit_id STRING,
    quantite STRING, prix_unitaire STRING, date_commande STRING,
    date_livraison_prevue STRING, date_livraison_reelle STRING,
    statut STRING, entrepot_destination STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_process_sanitaire (
    controle_id STRING, ligne_production STRING, type_controle STRING,
    date_controle STRING, resultat STRING, mesure_valeur STRING, mesure_unite STRING,
    seuil_min STRING, seuil_max STRING, operateur STRING, commentaire STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;
