-- ============================================================
-- FINANCE - Silver : Données financières nettoyées
-- Lakehouse : lh_finance
-- ============================================================

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
USING DELTA;

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
USING DELTA;
