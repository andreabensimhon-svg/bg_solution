-- ============================================================
-- FINANCE - Bronze : Données financières brutes
-- Lakehouse : lh_finance
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.raw_erp_finance (
    transaction_id      STRING,
    date_transaction    STRING,
    type_transaction    STRING,
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
COMMENT 'Transactions financières brutes depuis ERP';

CREATE TABLE IF NOT EXISTS bronze.raw_erp_bilans (
    bilan_id            STRING,
    annee_fiscale       STRING,
    type_bilan          STRING,
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
COMMENT 'Bilans comptables bruts depuis ERP';
