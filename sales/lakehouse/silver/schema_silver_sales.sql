-- SALES - Silver - Lakehouse : lh_sales

CREATE TABLE IF NOT EXISTS silver.clean_sales_orders (
    commande_id INT, client_id INT NOT NULL, date_commande DATE NOT NULL,
    date_livraison DATE, statut_commande STRING NOT NULL, montant_total_ht DECIMAL(12,2),
    remise_pct DECIMAL(5,2) DEFAULT 0, montant_apres_remise DECIMAL(12,2),
    commercial_id INT, canal_vente STRING, nb_lignes INT,
    _cleaned_ts TIMESTAMP, _source_batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.clean_sales_distributions (
    ligne_id INT, commande_id INT NOT NULL, produit_id INT NOT NULL,
    quantite INT NOT NULL, prix_unitaire_ht DECIMAL(10,2) NOT NULL,
    remise_ligne_pct DECIMAL(5,2) DEFAULT 0, montant_ligne_ht DECIMAL(12,2),
    _cleaned_ts TIMESTAMP, _source_batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver.clean_mails_parsed (
    mail_id INT, expediteur STRING, destinataire STRING, sujet STRING,
    corps_nettoye STRING, date_envoi TIMESTAMP, nb_pieces_jointes INT,
    langue_detectee STRING, sentiment_score DOUBLE,
    _cleaned_ts TIMESTAMP, _source_batch_id STRING
) USING DELTA;
