-- ============================================================
-- SHARED - Bronze : Référentiel commun
-- Lakehouse : lh_shared
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.raw_clients (
    client_id           STRING,
    raison_sociale      STRING,
    type_client         STRING,     -- 'grand_compte', 'garage_independant', 'distributeur'
    siret               STRING,
    adresse             STRING,
    code_postal         STRING,
    ville               STRING,
    pays                STRING,
    telephone           STRING,
    email               STRING,
    contact_principal   STRING,
    date_creation       STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Données brutes clients depuis la BDD partagée';

CREATE TABLE IF NOT EXISTS bronze.raw_produits (
    produit_id          STRING,
    nom_produit         STRING,
    categorie           STRING,     -- 'primer', 'peinture', 'mastic', 'vernis', 'accessoire'
    sous_categorie      STRING,
    reference           STRING,
    prix_catalogue_ht   STRING,
    unite_vente         STRING,
    poids_kg            STRING,
    fournisseur_id      STRING,
    actif               STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Référentiel produits depuis ERP';
