-- IoT - Bronze - Lakehouse : lh_iot

CREATE TABLE IF NOT EXISTS bronze.raw_iot_stream (
    event_id STRING, capteur_id STRING, type_mesure STRING,
    valeur STRING, unite STRING, timestamp_capteur STRING,
    ligne_production STRING, zone STRING, qualite_signal STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS bronze.raw_iot_images (
    image_id STRING, capteur_id STRING, chemin_fichier STRING,
    format_image STRING, taille_bytes STRING, date_capture STRING,
    ligne_production STRING, metadata_json STRING,
    _ingestion_ts TIMESTAMP, _source_file STRING, _batch_id STRING
) USING DELTA;
