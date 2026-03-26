-- ============================================================
-- BGSolutions - Lakehouse Bronze Layer Schema
-- Données brutes ingérées telles quelles depuis les sources
-- ============================================================

-- =====================
-- CLIENTS (BDD partagée)
-- =====================
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

-- =====================
-- ERP - FINANCE
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_erp_finance (
    transaction_id      STRING,
    date_transaction    STRING,
    type_transaction    STRING,     -- 'facture', 'avoir', 'paiement'
    client_id           STRING,
    montant_ht          STRING,
    montant_tva         STRING,
    montant_ttc         STRING,
    devise              STRING,
    compte_comptable    STRING,
    centre_cout         STRING,
    description         STRING,
    statut              STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Données brutes transactions financières depuis ERP';

CREATE TABLE IF NOT EXISTS bronze.raw_erp_bilans (
    bilan_id            STRING,
    annee_fiscale       STRING,
    type_bilan          STRING,     -- 'actif', 'passif', 'resultat'
    poste_comptable     STRING,
    libelle             STRING,
    montant             STRING,
    devise              STRING,
    date_cloture        STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Données brutes bilans comptables depuis ERP';

-- =====================
-- ERP - OPERATIONS
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_erp_operations (
    ordre_fabrication_id STRING,
    produit_id          STRING,
    ligne_production    STRING,
    quantite_prevue     STRING,
    quantite_produite   STRING,
    date_debut          STRING,
    date_fin            STRING,
    statut              STRING,     -- 'planifie', 'en_cours', 'termine', 'annule'
    operateur           STRING,
    commentaire         STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Données brutes ordres de fabrication depuis ERP';

CREATE TABLE IF NOT EXISTS bronze.raw_supply_chain (
    commande_fournisseur_id STRING,
    fournisseur_id      STRING,
    produit_id          STRING,
    quantite            STRING,
    prix_unitaire       STRING,
    date_commande       STRING,
    date_livraison_prevue STRING,
    date_livraison_reelle STRING,
    statut              STRING,
    entrepot_destination STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Données brutes supply chain depuis ERP';

CREATE TABLE IF NOT EXISTS bronze.raw_process_sanitaire (
    controle_id         STRING,
    ligne_production    STRING,
    type_controle       STRING,     -- 'qualite', 'securite', 'environnement'
    date_controle       STRING,
    resultat            STRING,     -- 'conforme', 'non_conforme', 'alerte'
    mesure_valeur       STRING,
    mesure_unite        STRING,
    seuil_min           STRING,
    seuil_max           STRING,
    operateur           STRING,
    commentaire         STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Données brutes contrôles process sanitaire';

-- =====================
-- ERP - VENTES (SALES)
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_erp_sales (
    commande_id         STRING,
    client_id           STRING,
    date_commande       STRING,
    date_livraison      STRING,
    statut_commande     STRING,
    montant_total_ht    STRING,
    remise_pct          STRING,
    commercial_id       STRING,
    canal_vente         STRING,     -- 'direct', 'distributeur', 'en_ligne'
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Données brutes commandes ventes depuis ERP';

CREATE TABLE IF NOT EXISTS bronze.raw_sales_lines (
    ligne_id            STRING,
    commande_id         STRING,
    produit_id          STRING,
    quantite            STRING,
    prix_unitaire_ht    STRING,
    remise_ligne_pct    STRING,
    montant_ligne_ht    STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Lignes de commande ventes depuis ERP';

-- =====================
-- JSON - COMMANDES
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_json_orders (
    order_payload       STRING,     -- JSON brut complet
    source_system       STRING,
    received_at         STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Commandes JSON reçues des systèmes tiers';

-- =====================
-- MAILS
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_mails (
    mail_id             STRING,
    expediteur          STRING,
    destinataire        STRING,
    sujet               STRING,
    corps               STRING,     -- Texte brut du mail
    date_envoi          STRING,
    pieces_jointes      STRING,     -- Liste JSON des PJ
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Corps de mails bruts pour analyse NLP';

-- =====================
-- PLANS DE SUPPLY
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_supply_plans (
    plan_id             STRING,
    nom_fichier         STRING,
    contenu_texte       STRING,     -- Texte extrait du document
    format_source       STRING,     -- 'pdf', 'excel', 'word'
    date_document       STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Plans de supply extraits (texte brut)';

-- =====================
-- IoT - IMAGES
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_iot_images (
    image_id            STRING,
    capteur_id          STRING,
    chemin_fichier      STRING,     -- Chemin dans le lakehouse Files/
    format_image        STRING,     -- 'jpeg', 'png'
    taille_bytes        STRING,
    date_capture        STRING,
    ligne_production    STRING,
    metadata_json       STRING,     -- Métadonnées capteur en JSON
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Métadonnées images capteurs IoT';

-- =====================
-- IoT - STREAM TEMPS RÉEL
-- =====================
CREATE TABLE IF NOT EXISTS bronze.raw_iot_stream (
    event_id            STRING,
    capteur_id          STRING,
    type_mesure         STRING,     -- 'temperature', 'pression', 'humidite'
    valeur              STRING,
    unite               STRING,
    timestamp_capteur   STRING,
    ligne_production    STRING,
    zone                STRING,
    qualite_signal      STRING,
    _ingestion_ts       TIMESTAMP,
    _source_file        STRING,
    _batch_id           STRING
)
USING DELTA
COMMENT 'Flux temps réel capteurs IoT depuis Eventstream';

-- =====================
-- PRODUITS (référentiel)
-- =====================
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
