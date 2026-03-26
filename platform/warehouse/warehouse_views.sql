-- ============================================================
-- Warehouse : Vues cross-domaine OLAP (wh_bgsolutions)
-- Requêtes cross-Lakehouse via Shortcuts ou SQL endpoints
-- ============================================================

-- === FINANCE ===
CREATE SCHEMA finance;
CREATE VIEW finance.v_marges_mensuelles AS SELECT * FROM lh_finance.gold.fact_marges;
CREATE VIEW finance.v_forecast_vs_reel AS SELECT * FROM lh_finance.gold.fact_forecast;
CREATE VIEW finance.v_bilans_comparatifs AS SELECT * FROM lh_finance.gold.dim_bilans_comptables;

-- === OPERATIONS ===
CREATE SCHEMA operations;
CREATE VIEW operations.v_production_kpi AS SELECT * FROM lh_operations.gold.fact_production;
CREATE VIEW operations.v_supply_chain AS SELECT * FROM lh_operations.gold.fact_supply_chain;
CREATE VIEW operations.v_sanitaire AS SELECT * FROM lh_operations.gold.fact_process_sanitaire;

-- === SALES ===
CREATE SCHEMA sales;
CREATE VIEW sales.v_ventes_performance AS SELECT * FROM lh_sales.gold.fact_ventes;
CREATE VIEW sales.v_grands_comptes AS SELECT * FROM lh_sales.gold.dim_clients_grands_comptes;

-- === IOT ===
CREATE SCHEMA iot;
CREATE VIEW iot.v_capteurs_resume AS SELECT * FROM lh_iot.gold.fact_iot_realtime;
CREATE VIEW iot.v_anomalies AS SELECT * FROM lh_iot.gold.fact_iot_anomalies;

-- === SHARED ===
CREATE SCHEMA shared;
CREATE VIEW shared.v_clients AS SELECT * FROM lh_shared.gold.dim_clients;
CREATE VIEW shared.v_products AS SELECT * FROM lh_shared.gold.dim_products;

-- === CROSS-DOMAIN : Vue consolidée CA par domaine ===
CREATE VIEW shared.v_kpi_dashboard AS
SELECT 'finance' AS domaine, annee, mois, SUM(chiffre_affaires_ht) AS montant FROM lh_finance.gold.fact_marges GROUP BY annee, mois
UNION ALL
SELECT 'sales', annee, mois, SUM(ca_net_ht) FROM lh_sales.gold.fact_ventes GROUP BY annee, mois;
