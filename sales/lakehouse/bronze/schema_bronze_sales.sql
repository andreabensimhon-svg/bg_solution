-- ============================================================
-- SALES - Bronze
-- Lakehouse : lh_sales
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.raw_erp_sales (
    commande_id STRING, client_id STRING, date_commande STRING, date_livraison STRING,
    statut_commande STRING, montant_total_ht STRING, remise_pct STRING,
    commercial_id STRING, canal_vente STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_sales_lines (
    ligne_id STRING, commande_id STRING, produit_id STRING,
    quantite STRING, prix_unitaire_ht STRING, remise_ligne_pct STRING, montant_ligne_ht STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_json_orders (
    order_payload STRING, source_system STRING, received_at STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_mails (
    mail_id STRING, expediteur STRING, destinataire STRING, sujet STRING,
    corps STRING, date_envoi STRING, pieces_jointes STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;
