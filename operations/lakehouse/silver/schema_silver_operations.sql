-- ============================================================
-- OPERATIONS - Silver
-- Lakehouse : lh_operations
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.clean_operations_production (
    ordre_fabrication_id INT, produit_id INT NOT NULL, ligne_production STRING NOT NULL,
    quantite_prevue INT, quantite_produite INT, date_debut TIMESTAMP, date_fin TIMESTAMP,
    duree_heures DECIMAL(8,2), statut STRING NOT NULL, operateur STRING,
    taux_rendement DECIMAL(5,2), _cleaned_ts TIMESTAMP, _source_batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.clean_supply_chain (
    commande_fournisseur_id INT, fournisseur_id INT NOT NULL, produit_id INT NOT NULL,
    quantite INT NOT NULL, prix_unitaire DECIMAL(10,2), date_commande DATE NOT NULL,
    date_livraison_prevue DATE, date_livraison_reelle DATE, retard_jours INT,
    statut STRING NOT NULL, entrepot_destination STRING,
    _cleaned_ts TIMESTAMP, _source_batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.clean_process_sanitaire (
    controle_id INT, ligne_production STRING NOT NULL, type_controle STRING NOT NULL,
    date_controle TIMESTAMP NOT NULL, resultat STRING NOT NULL,
    mesure_valeur DECIMAL(10,4), mesure_unite STRING, seuil_min DECIMAL(10,4),
    seuil_max DECIMAL(10,4), is_dans_seuils BOOLEAN, operateur STRING, commentaire STRING,
    _cleaned_ts TIMESTAMP, _source_batch_id STRING
) USING DELTA;
