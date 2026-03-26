-- SQL DB : Tables OLTP applicatives (sqldb_app)

CREATE SCHEMA app;

CREATE TABLE app.utilisateurs (
    utilisateur_id INT IDENTITY(1,1) PRIMARY KEY,
    nom NVARCHAR(100) NOT NULL, prenom NVARCHAR(100) NOT NULL,
    email NVARCHAR(255) NOT NULL UNIQUE,
    role NVARCHAR(50) NOT NULL CHECK (role IN ('admin', 'commercial', 'operateur', 'finance', 'lecture')),
    entite NVARCHAR(50) NOT NULL CHECK (entite IN ('Finance', 'Operations', 'Sales', 'IT')),
    date_creation DATETIME2 DEFAULT GETUTCDATE(), is_active BIT DEFAULT 1
);

CREATE TABLE app.audit_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    timestamp_utc DATETIME2 DEFAULT GETUTCDATE(),
    utilisateur_id INT REFERENCES app.utilisateurs(utilisateur_id),
    action NVARCHAR(50) NOT NULL, table_cible NVARCHAR(100),
    enregistrement_id NVARCHAR(100), details NVARCHAR(MAX)
);
CREATE INDEX IX_audit_timestamp ON app.audit_log(timestamp_utc DESC);

CREATE TABLE app.alertes (
    alerte_id INT IDENTITY(1,1) PRIMARY KEY,
    type_alerte NVARCHAR(50) NOT NULL CHECK (type_alerte IN ('iot_anomalie', 'seuil_finance', 'retard_supply', 'non_conformite', 'systeme')),
    severite NVARCHAR(20) NOT NULL CHECK (severite IN ('info', 'warning', 'critical')),
    titre NVARCHAR(200) NOT NULL, message NVARCHAR(MAX),
    source_domaine NVARCHAR(50), reference_id NVARCHAR(100),
    date_creation DATETIME2 DEFAULT GETUTCDATE(), is_lu BIT DEFAULT 0
);

CREATE TABLE app.seuils_iot (
    seuil_id INT IDENTITY(1,1) PRIMARY KEY,
    capteur_type NVARCHAR(50) NOT NULL, type_mesure NVARCHAR(50) NOT NULL,
    seuil_warning DECIMAL(10,4), seuil_critical DECIMAL(10,4),
    unite NVARCHAR(20),
    CONSTRAINT UQ_seuil UNIQUE (capteur_type, type_mesure)
);

INSERT INTO app.seuils_iot VALUES
('PROD', 'temperature', 150, 180, 'celsius'),
('PROD', 'pression', 300, 400, 'bar'),
('PROD', 'humidite', 85, 95, 'pct'),
('STOCK', 'temperature', 30, 40, 'celsius'),
('LABO', 'temperature', 25, 30, 'celsius');
