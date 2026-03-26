-- ============================================================
-- SHARED - Gold : Dimensions partagées
-- Lakehouse : lh_shared (exposé via Shortcuts aux autres domaines)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_products (
    produit_id          INT             NOT NULL,
    nom_produit         STRING          NOT NULL,
    categorie           STRING          NOT NULL,
    sous_categorie      STRING,
    reference           STRING,
    prix_catalogue_ht   DECIMAL(10,2),
    unite_vente         STRING,
    poids_kg            DECIMAL(8,3),
    nb_ventes_total     INT,
    ca_total_ht         DECIMAL(15,2),
    marge_moyenne_pct   DECIMAL(5,2),
    classement_ventes   INT,
    is_active           BOOLEAN,
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Référentiel produits enrichi - partagé via Shortcuts';

CREATE TABLE IF NOT EXISTS gold.dim_clients (
    client_id           INT             NOT NULL,
    raison_sociale      STRING,
    type_client         STRING,
    ville               STRING,
    pays                STRING,
    date_premier_achat  DATE,
    date_dernier_achat  DATE,
    nb_commandes_total  INT,
    ca_cumule_ht        DECIMAL(15,2),
    ca_12_mois          DECIMAL(15,2),
    panier_moyen        DECIMAL(10,2),
    categorie_abc       STRING,
    score_fidelite      DECIMAL(5,2),
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Dimension clients enrichie - partagée via Shortcuts';
