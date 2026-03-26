-- SALES - Gold - Lakehouse : lh_sales
-- Shortcuts : shared_dim_clients, shared_dim_products

CREATE TABLE IF NOT EXISTS gold.fact_ventes (
    vente_id BIGINT GENERATED ALWAYS AS IDENTITY,
    date_vente DATE NOT NULL, annee INT, mois INT, trimestre INT,
    client_id INT, type_client STRING, produit_id INT, categorie_produit STRING,
    canal_vente STRING, commercial_id INT, nb_commandes INT,
    quantite_totale INT, ca_brut_ht DECIMAL(15,2), remise_totale DECIMAL(15,2),
    ca_net_ht DECIMAL(15,2), panier_moyen DECIMAL(10,2), _computed_ts TIMESTAMP
) USING DELTA PARTITIONED BY (annee, mois);

CREATE TABLE IF NOT EXISTS gold.dim_clients_grands_comptes (
    grand_compte_id BIGINT GENERATED ALWAYS AS IDENTITY,
    client_id INT NOT NULL, raison_sociale STRING,
    date_periode DATE NOT NULL, annee INT, mois INT,
    ca_mensuel_ht DECIMAL(15,2), ca_cumul_annee DECIMAL(15,2),
    objectif_mensuel DECIMAL(15,2), atteinte_objectif_pct DECIMAL(5,2),
    nb_commandes INT, top_produit STRING, _computed_ts TIMESTAMP
) USING DELTA;
