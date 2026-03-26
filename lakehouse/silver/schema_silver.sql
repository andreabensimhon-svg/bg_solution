-- ============================================================
-- BGSolutions - Lakehouse Silver Layer Schema
-- Données nettoyées, validées, dédupliquées et typées
-- ============================================================

-- =====================
-- CLIENTS
-- =====================
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

-- =====================
-- PRODUITS
-- =====================
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

-- =====================
-- FINANCE
-- =====================
CREATE TABLE IF NOT EXISTS silver.clean_finance_transactions (
    transaction_id      INT,
    date_transaction    DATE            NOT NULL,
    type_transaction    STRING          NOT NULL,
    client_id           INT,
    montant_ht          DECIMAL(12,2)   NOT NULL,
    montant_tva         DECIMAL(12,2),
    montant_ttc         DECIMAL(12,2),
    devise              STRING          DEFAULT 'EUR',
    compte_comptable    STRING,
    centre_cout         STRING,
    description         STRING,
    statut              STRING,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Transactions financières nettoyées';

CREATE TABLE IF NOT EXISTS silver.clean_bilans_comptables (
    bilan_id            INT,
    annee_fiscale       INT             NOT NULL,
    type_bilan          STRING          NOT NULL,
    poste_comptable     STRING          NOT NULL,
    libelle             STRING,
    montant             DECIMAL(15,2)   NOT NULL,
    devise              STRING          DEFAULT 'EUR',
    date_cloture        DATE,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Bilans comptables nettoyés';

-- =====================
-- OPERATIONS
-- =====================
CREATE TABLE IF NOT EXISTS silver.clean_operations_production (
    ordre_fabrication_id INT,
    produit_id          INT             NOT NULL,
    ligne_production    STRING          NOT NULL,
    quantite_prevue     INT,
    quantite_produite   INT,
    date_debut          TIMESTAMP,
    date_fin            TIMESTAMP,
    duree_heures        DECIMAL(8,2),
    statut              STRING          NOT NULL,
    operateur           STRING,
    taux_rendement      DECIMAL(5,2),
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Ordres de fabrication nettoyés avec calcul de rendement';

CREATE TABLE IF NOT EXISTS silver.clean_supply_chain (
    commande_fournisseur_id INT,
    fournisseur_id      INT             NOT NULL,
    produit_id          INT             NOT NULL,
    quantite            INT             NOT NULL,
    prix_unitaire       DECIMAL(10,2),
    date_commande       DATE            NOT NULL,
    date_livraison_prevue DATE,
    date_livraison_reelle DATE,
    retard_jours        INT,
    statut              STRING          NOT NULL,
    entrepot_destination STRING,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Supply chain nettoyé avec calcul retard';

CREATE TABLE IF NOT EXISTS silver.clean_process_sanitaire (
    controle_id         INT,
    ligne_production    STRING          NOT NULL,
    type_controle       STRING          NOT NULL,
    date_controle       TIMESTAMP       NOT NULL,
    resultat            STRING          NOT NULL,
    mesure_valeur       DECIMAL(10,4),
    mesure_unite        STRING,
    seuil_min           DECIMAL(10,4),
    seuil_max           DECIMAL(10,4),
    is_dans_seuils      BOOLEAN,
    operateur           STRING,
    commentaire         STRING,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Contrôles process sanitaire nettoyés';

-- =====================
-- SALES
-- =====================
CREATE TABLE IF NOT EXISTS silver.clean_sales_orders (
    commande_id         INT,
    client_id           INT             NOT NULL,
    date_commande       DATE            NOT NULL,
    date_livraison      DATE,
    statut_commande     STRING          NOT NULL,
    montant_total_ht    DECIMAL(12,2),
    remise_pct          DECIMAL(5,2)    DEFAULT 0,
    montant_apres_remise DECIMAL(12,2),
    commercial_id       INT,
    canal_vente         STRING,
    nb_lignes           INT,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Commandes ventes nettoyées';

CREATE TABLE IF NOT EXISTS silver.clean_sales_distributions (
    ligne_id            INT,
    commande_id         INT             NOT NULL,
    produit_id          INT             NOT NULL,
    quantite            INT             NOT NULL,
    prix_unitaire_ht    DECIMAL(10,2)   NOT NULL,
    remise_ligne_pct    DECIMAL(5,2)    DEFAULT 0,
    montant_ligne_ht    DECIMAL(12,2),
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Lignes de commande ventes nettoyées';

-- =====================
-- IoT
-- =====================
CREATE TABLE IF NOT EXISTS silver.clean_iot_measures (
    event_id            BIGINT,
    capteur_id          STRING          NOT NULL,
    type_mesure         STRING          NOT NULL,
    valeur              DOUBLE          NOT NULL,
    unite               STRING          NOT NULL,
    timestamp_capteur   TIMESTAMP       NOT NULL,
    ligne_production    STRING,
    zone                STRING,
    qualite_signal      DOUBLE,
    is_anomalie         BOOLEAN         DEFAULT FALSE,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Mesures IoT nettoyées et typées';

-- =====================
-- MAILS (parsés)
-- =====================
CREATE TABLE IF NOT EXISTS silver.clean_mails_parsed (
    mail_id             INT,
    expediteur          STRING,
    destinataire        STRING,
    sujet               STRING,
    corps_nettoye       STRING,
    date_envoi          TIMESTAMP,
    nb_pieces_jointes   INT,
    langue_detectee     STRING,
    sentiment_score     DOUBLE,
    _cleaned_ts         TIMESTAMP,
    _source_batch_id    STRING
)
USING DELTA
COMMENT 'Mails parsés et enrichis avec analyse NLP';
