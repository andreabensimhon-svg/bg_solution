-- ============================================================
-- BGSolutions - SQL Database Schema
-- Tables OLTP pour les applications transactionnelles
-- ============================================================

-- =====================
-- SCHÉMA : app
-- =====================

CREATE SCHEMA IF NOT EXISTS app;

-- =====================
-- TABLE : Utilisateurs de l'application
-- =====================
CREATE TABLE app.utilisateurs (
    utilisateur_id      INT IDENTITY(1,1) PRIMARY KEY,
    nom                 NVARCHAR(100)     NOT NULL,
    prenom              NVARCHAR(100)     NOT NULL,
    email               NVARCHAR(255)     NOT NULL UNIQUE,
    role                NVARCHAR(50)      NOT NULL CHECK (role IN ('admin', 'commercial', 'operateur', 'finance', 'lecture')),
    entite              NVARCHAR(50)      NOT NULL CHECK (entite IN ('Finance', 'Operations', 'Sales', 'IT')),
    date_creation       DATETIME2         DEFAULT GETUTCDATE(),
    dernier_acces       DATETIME2,
    is_active           BIT               DEFAULT 1
);

-- =====================
-- TABLE : Paramètres applicatifs
-- =====================
CREATE TABLE app.parametres (
    parametre_id        INT IDENTITY(1,1) PRIMARY KEY,
    cle                 NVARCHAR(100)     NOT NULL UNIQUE,
    valeur              NVARCHAR(MAX)     NOT NULL,
    type_donnee         NVARCHAR(20)      NOT NULL CHECK (type_donnee IN ('string', 'int', 'decimal', 'boolean', 'json')),
    description         NVARCHAR(500),
    modifie_par         INT               REFERENCES app.utilisateurs(utilisateur_id),
    date_modification   DATETIME2         DEFAULT GETUTCDATE()
);

-- =====================
-- TABLE : Journal d'audit
-- =====================
CREATE TABLE app.audit_log (
    log_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    timestamp_utc       DATETIME2         DEFAULT GETUTCDATE(),
    utilisateur_id      INT               REFERENCES app.utilisateurs(utilisateur_id),
    action              NVARCHAR(50)      NOT NULL,
    table_cible         NVARCHAR(100),
    enregistrement_id   NVARCHAR(100),
    details             NVARCHAR(MAX),
    adresse_ip          NVARCHAR(45)
);

CREATE INDEX IX_audit_log_timestamp ON app.audit_log(timestamp_utc DESC);
CREATE INDEX IX_audit_log_utilisateur ON app.audit_log(utilisateur_id);

-- =====================
-- TABLE : Alertes et notifications
-- =====================
CREATE TABLE app.alertes (
    alerte_id           INT IDENTITY(1,1) PRIMARY KEY,
    type_alerte         NVARCHAR(50)      NOT NULL CHECK (type_alerte IN ('iot_anomalie', 'seuil_finance', 'retard_supply', 'non_conformite', 'systeme')),
    severite            NVARCHAR(20)      NOT NULL CHECK (severite IN ('info', 'warning', 'critical')),
    titre               NVARCHAR(200)     NOT NULL,
    message             NVARCHAR(MAX),
    source              NVARCHAR(100),
    reference_id        NVARCHAR(100),
    date_creation       DATETIME2         DEFAULT GETUTCDATE(),
    date_acquittement   DATETIME2,
    acquitte_par        INT               REFERENCES app.utilisateurs(utilisateur_id),
    is_lu               BIT               DEFAULT 0
);

CREATE INDEX IX_alertes_type ON app.alertes(type_alerte, severite);
CREATE INDEX IX_alertes_non_lues ON app.alertes(is_lu) WHERE is_lu = 0;

-- =====================
-- TABLE : Objectifs commerciaux
-- =====================
CREATE TABLE app.objectifs_commerciaux (
    objectif_id         INT IDENTITY(1,1) PRIMARY KEY,
    client_id           INT               NOT NULL,
    annee               INT               NOT NULL,
    mois                INT               NOT NULL,
    objectif_ca_ht      DECIMAL(15,2)     NOT NULL,
    commercial_id       INT,
    commentaire         NVARCHAR(500),
    date_creation       DATETIME2         DEFAULT GETUTCDATE(),
    CONSTRAINT UQ_objectif_client_periode UNIQUE (client_id, annee, mois)
);

-- =====================
-- TABLE : Configuration des seuils IoT
-- =====================
CREATE TABLE app.seuils_iot (
    seuil_id            INT IDENTITY(1,1) PRIMARY KEY,
    capteur_type        NVARCHAR(50)      NOT NULL,
    type_mesure         NVARCHAR(50)      NOT NULL,
    seuil_warning       DECIMAL(10,4),
    seuil_critical      DECIMAL(10,4),
    seuil_min           DECIMAL(10,4),
    seuil_max           DECIMAL(10,4),
    unite               NVARCHAR(20),
    date_modification   DATETIME2         DEFAULT GETUTCDATE(),
    CONSTRAINT UQ_seuil_capteur_mesure UNIQUE (capteur_type, type_mesure)
);

-- Données initiales seuils IoT
INSERT INTO app.seuils_iot (capteur_type, type_mesure, seuil_warning, seuil_critical, seuil_min, seuil_max, unite) VALUES
('PROD', 'temperature', 150, 180, -10, 200, 'celsius'),
('PROD', 'pression', 300, 400, 0.5, 500, 'bar'),
('PROD', 'humidite', 85, 95, 5, 100, 'pct'),
('STOCK', 'temperature', 30, 40, -5, 50, 'celsius'),
('STOCK', 'humidite', 75, 90, 10, 100, 'pct'),
('LABO', 'temperature', 25, 30, 15, 35, 'celsius'),
('LABO', 'pression', 200, 300, 0, 500, 'bar');
