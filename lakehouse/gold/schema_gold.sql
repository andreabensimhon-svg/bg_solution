-- ============================================================
-- BGSolutions - Lakehouse Gold Layer Schema
-- Données métier agrégées prêtes pour la consommation
-- ============================================================

-- ========================
-- FINANCE
-- ========================

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
COMMENT 'Calculs de marges par client, produit et période';

CREATE TABLE IF NOT EXISTS gold.fact_forecast (
    forecast_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    date_forecast       DATE            NOT NULL,
    horizon_mois        INT             NOT NULL,
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
USING DELTA
COMMENT 'Prévisions financières avec intervalles de confiance';

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
USING DELTA
COMMENT '3 bilans comptables annuels avec variation N-1';

-- ========================
-- OPERATIONS
-- ========================

CREATE TABLE IF NOT EXISTS gold.fact_production (
    production_id       BIGINT GENERATED ALWAYS AS IDENTITY,
    date_production     DATE            NOT NULL,
    annee               INT,
    mois                INT,
    semaine             INT,
    ligne_production    STRING          NOT NULL,
    produit_id          INT,
    categorie_produit   STRING,
    nb_ordres           INT,
    quantite_prevue_total INT,
    quantite_produite_total INT,
    taux_rendement_moyen DECIMAL(5,2),
    duree_totale_heures DECIMAL(10,2),
    nb_incidents        INT             DEFAULT 0,
    _computed_ts        TIMESTAMP
)
USING DELTA
PARTITIONED BY (annee, mois)
COMMENT 'KPIs production agrégés par ligne et période';

CREATE TABLE IF NOT EXISTS gold.fact_supply_chain (
    supply_id           BIGINT GENERATED ALWAYS AS IDENTITY,
    date_periode        DATE            NOT NULL,
    annee               INT,
    mois                INT,
    fournisseur_id      INT,
    produit_id          INT,
    categorie_produit   STRING,
    nb_commandes        INT,
    quantite_totale     INT,
    montant_total       DECIMAL(15,2),
    delai_moyen_jours   DECIMAL(5,1),
    taux_retard_pct     DECIMAL(5,2),
    taux_conformite_pct DECIMAL(5,2),
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Optimisation chaîne logistique par fournisseur et produit';

CREATE TABLE IF NOT EXISTS gold.fact_process_sanitaire (
    sanitaire_id        BIGINT GENERATED ALWAYS AS IDENTITY,
    date_periode        DATE            NOT NULL,
    annee               INT,
    mois                INT,
    ligne_production    STRING          NOT NULL,
    type_controle       STRING          NOT NULL,
    nb_controles        INT,
    nb_conformes        INT,
    nb_non_conformes    INT,
    nb_alertes          INT,
    taux_conformite_pct DECIMAL(5,2),
    valeur_moyenne      DECIMAL(10,4),
    valeur_min          DECIMAL(10,4),
    valeur_max          DECIMAL(10,4),
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Suivi process sanitaire agrégé par ligne et type';

-- ========================
-- SALES
-- ========================

CREATE TABLE IF NOT EXISTS gold.fact_ventes (
    vente_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    date_vente          DATE            NOT NULL,
    annee               INT,
    mois                INT,
    trimestre           INT,
    client_id           INT,
    type_client         STRING,
    produit_id          INT,
    categorie_produit   STRING,
    canal_vente         STRING,
    commercial_id       INT,
    nb_commandes        INT,
    quantite_totale     INT,
    ca_brut_ht          DECIMAL(15,2),
    remise_totale       DECIMAL(15,2),
    ca_net_ht           DECIMAL(15,2),
    panier_moyen        DECIMAL(10,2),
    _computed_ts        TIMESTAMP
)
USING DELTA
PARTITIONED BY (annee, mois)
COMMENT 'Ventes et distributions agrégées';

CREATE TABLE IF NOT EXISTS gold.fact_clients (
    client_agg_id       BIGINT GENERATED ALWAYS AS IDENTITY,
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
    categorie_abc       STRING,         -- 'A', 'B', 'C'
    score_fidelite      DECIMAL(5,2),
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Analyse clients avec segmentation ABC et scoring';

CREATE TABLE IF NOT EXISTS gold.dim_clients_grands_comptes (
    grand_compte_id     BIGINT GENERATED ALWAYS AS IDENTITY,
    client_id           INT             NOT NULL,
    raison_sociale      STRING,
    date_periode        DATE            NOT NULL,
    annee               INT,
    mois                INT,
    ca_mensuel_ht       DECIMAL(15,2),
    ca_cumul_annee      DECIMAL(15,2),
    objectif_mensuel    DECIMAL(15,2),
    atteinte_objectif_pct DECIMAL(5,2),
    nb_commandes        INT,
    top_produit         STRING,
    satisfaction_score  DECIMAL(5,2),
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Suivi dédié grands comptes avec objectifs';

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
COMMENT 'Référentiel produits enrichi avec métriques ventes';

-- ========================
-- IoT
-- ========================

CREATE TABLE IF NOT EXISTS gold.fact_iot_realtime (
    iot_agg_id          BIGINT GENERATED ALWAYS AS IDENTITY,
    window_start        TIMESTAMP       NOT NULL,
    window_end          TIMESTAMP       NOT NULL,
    capteur_id          STRING          NOT NULL,
    type_mesure         STRING          NOT NULL,
    ligne_production    STRING,
    zone                STRING,
    valeur_moyenne      DOUBLE,
    valeur_min          DOUBLE,
    valeur_max          DOUBLE,
    ecart_type          DOUBLE,
    nb_mesures          INT,
    qualite_signal_moy  DOUBLE,
    has_anomalie        BOOLEAN         DEFAULT FALSE,
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Mesures capteurs agrégées par fenêtre temporelle';

CREATE TABLE IF NOT EXISTS gold.fact_iot_anomalies (
    anomalie_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    capteur_id          STRING          NOT NULL,
    type_mesure         STRING          NOT NULL,
    timestamp_detection TIMESTAMP       NOT NULL,
    valeur_anomalie     DOUBLE,
    valeur_attendue     DOUBLE,
    ecart_pct           DECIMAL(5,2),
    severite            STRING,         -- 'info', 'warning', 'critical'
    ligne_production    STRING,
    zone                STRING,
    modele_detection    STRING,
    is_confirmee        BOOLEAN         DEFAULT FALSE,
    _computed_ts        TIMESTAMP
)
USING DELTA
COMMENT 'Anomalies détectées par les modèles ML sur capteurs IoT';
