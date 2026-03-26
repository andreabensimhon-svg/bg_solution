-- ============================================================
-- SHARED - Silver : Référentiel commun nettoyé
-- Lakehouse : lh_shared
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.clean_clients (
    client_id           INT,
    raison_sociale      STRING          NOT NULL,
    type_client         STRING          NOT NULL,
    siret               STRING,
    adresse             STRING,
    code_postal         STRING,
    ville               STRING,
    pays                STRING          DEFAULT 'France',
    telephone           STRING,
    email               STRING,
    contact_principal   STRING,
    date_creation       DATE,
    is_active           BOOLEAN         DEFAULT TRUE,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Clients nettoyés et dédupliqués';

CREATE TABLE IF NOT EXISTS silver.clean_produits (
    produit_id          INT,
    nom_produit         STRING          NOT NULL,
    categorie           STRING          NOT NULL,
    sous_categorie      STRING,
    reference           STRING,
    prix_catalogue_ht   DECIMAL(10,2),
    unite_vente         STRING,
    poids_kg            DECIMAL(8,3),
    fournisseur_id      INT,
    is_active           BOOLEAN         DEFAULT TRUE,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Référentiel produits nettoyé';
