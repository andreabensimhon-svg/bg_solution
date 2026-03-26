-- IoT - Gold - Lakehouse : lh_iot

CREATE TABLE IF NOT EXISTS gold.fact_iot_realtime (
    iot_agg_id BIGINT GENERATED ALWAYS AS IDENTITY,
    window_start TIMESTAMP NOT NULL, window_end TIMESTAMP NOT NULL,
    capteur_id STRING NOT NULL, type_mesure STRING NOT NULL,
    ligne_production STRING, zone STRING,
    valeur_moyenne DOUBLE, valeur_min DOUBLE, valeur_max DOUBLE,
    ecart_type DOUBLE, nb_mesures INT, qualite_signal_moy DOUBLE,
    has_anomalie BOOLEAN DEFAULT FALSE, _computed_ts TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS gold.fact_iot_anomalies (
    anomalie_id BIGINT GENERATED ALWAYS AS IDENTITY,
    capteur_id STRING NOT NULL, type_mesure STRING NOT NULL,
    timestamp_detection TIMESTAMP NOT NULL,
    valeur_anomalie DOUBLE, valeur_attendue DOUBLE, ecart_pct DECIMAL(5,2),
    severite STRING, ligne_production STRING, zone STRING,
    modele_detection STRING, is_confirmee BOOLEAN DEFAULT FALSE,
    _computed_ts TIMESTAMP
) USING DELTA;
