-- ============================================================
-- FINANCE - Gold : KPIs Finance
-- Lakehouse : lh_finance
-- Shortcuts : shared_dim_clients, shared_dim_products
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_marges (
    marge_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    date_calcul         DATE            NOT NULL,
    annee               INT,
    mois                INT,
    trimestre           INT,
    client_id           INT,
    produit_id          INT,
    categorie_produit   STRING,
    chiffre_affaires_ht DECIMAL(15,2),
    cout_revient        DECIMAL(15,2),
    marge_brute         DECIMAL(15,2),
    marge_brute_pct     DECIMAL(5,2),
    marge_nette         DECIMAL(15,2),
    marge_nette_pct     DECIMAL(5,2),
    volume_vendu        INT,
    _computed_ts        TIMESTAMP
)
USING DELTA
PARTITIONED BY (annee, mois)
COMMENT 'Marges par client/produit/période';

CREATE TABLE IF NOT EXISTS gold.fact_forecast (
    forecast_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    date_forecast       DATE            NOT NULL,
    horizon_mois        INT,
    annee_cible         INT,
    mois_cible          INT,
    categorie_produit   STRING,
    ca_prevu            DECIMAL(15,2),
    ca_reel             DECIMAL(15,2),
    ecart_pct           DECIMAL(5,2),
    intervalle_confiance_bas DECIMAL(15,2),
    intervalle_confiance_haut DECIMAL(15,2),
    modele_utilise      STRING,
    _computed_ts        TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_bilans_comptables (
    bilan_id            INT,
    annee_fiscale       INT             NOT NULL,
    type_bilan          STRING          NOT NULL,
    poste_comptable     STRING          NOT NULL,
    libelle             STRING,
    montant             DECIMAL(15,2)   NOT NULL,
    montant_n_moins_1   DECIMAL(15,2),
    variation_pct       DECIMAL(5,2),
    devise              STRING          DEFAULT 'EUR',
    date_cloture        DATE,
    _computed_ts        TIMESTAMP
)
USING DELTA;
