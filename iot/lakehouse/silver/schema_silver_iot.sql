-- IoT - Silver - Lakehouse : lh_iot

CREATE TABLE IF NOT EXISTS silver.clean_iot_measures (
    event_id BIGINT, capteur_id STRING NOT NULL, type_mesure STRING NOT NULL,
    valeur DOUBLE NOT NULL, unite STRING NOT NULL, timestamp_capteur TIMESTAMP NOT NULL,
    ligne_production STRING, zone STRING, qualite_signal DOUBLE,
    is_anomalie BOOLEAN DEFAULT FALSE,
    _cleaned_ts TIMESTAMP, _source_batch_id STRING
) USING DELTA;
