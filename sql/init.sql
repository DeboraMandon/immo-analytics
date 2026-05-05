-- ============================================================
-- immo-analytics — Schéma base de données
-- PostgreSQL 15 + PostGIS
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── communes ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS communes (
    code_insee           VARCHAR(10) PRIMARY KEY,
    nom                  VARCHAR(200) NOT NULL,
    departement          VARCHAR(3),
    region               VARCHAR(100),
    population           INTEGER,
    latitude             NUMERIC(9,6),
    longitude            NUMERIC(9,6),
    geom                 GEOMETRY(Point, 4326),
    revenu_median_annuel NUMERIC(10,2),
    revenu_annee_ref     INTEGER,
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_communes_nom  ON communes USING GIN (nom gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_communes_geom ON communes USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_communes_dept ON communes (departement);

-- ── prix_immobilier ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prix_immobilier (
    id              SERIAL PRIMARY KEY,
    code_insee      VARCHAR(10) NOT NULL REFERENCES communes(code_insee) ON DELETE CASCADE,
    annee           SMALLINT NOT NULL,
    trimestre       SMALLINT,
    type_bien       VARCHAR(20) NOT NULL,
    prix_m2_moyen   NUMERIC(10,2),
    prix_m2_median  NUMERIC(10,2),
    nb_transactions INTEGER,
    source          VARCHAR(50) DEFAULT 'DVF',
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (code_insee, annee, trimestre, type_bien)
);

CREATE INDEX IF NOT EXISTS idx_prix_commune ON prix_immobilier (code_insee);
CREATE INDEX IF NOT EXISTS idx_prix_annee   ON prix_immobilier (annee);

-- ── taux_nationaux ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS taux_nationaux (
    id          SERIAL PRIMARY KEY,
    date_obs    DATE NOT NULL,
    duree_ans   SMALLINT NOT NULL,
    taux_moyen  NUMERIC(5,3) NOT NULL,
    taux_min    NUMERIC(5,3),
    taux_max    NUMERIC(5,3),
    taux_usure  NUMERIC(5,3),
    source      VARCHAR(100) NOT NULL,
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (date_obs, duree_ans, source)
);

CREATE INDEX IF NOT EXISTS idx_taux_date  ON taux_nationaux (date_obs DESC);

-- ── oat_historique ────────────────────────────────────────────
-- OAT 10 ans France — Banque de France
-- Référence principale pour les taux de crédit immobilier
CREATE TABLE IF NOT EXISTS oat_historique (
    id          SERIAL PRIMARY KEY,
    date_obs    DATE NOT NULL UNIQUE,
    valeur      NUMERIC(6,3) NOT NULL,  -- % ex: 3.750
    source      VARCHAR(100) DEFAULT 'Banque de France',
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oat_date ON oat_historique (date_obs DESC);

-- ── predictions_taux ──────────────────────────────────────────
-- Prédictions Prophet des taux sur 6 mois
CREATE TABLE IF NOT EXISTS predictions_taux (
    id               SERIAL PRIMARY KEY,
    date_prediction  DATE NOT NULL,
    duree_ans        SMALLINT NOT NULL,
    taux_predit      NUMERIC(5,3) NOT NULL,
    taux_lower       NUMERIC(5,3),   -- intervalle de confiance bas (80%)
    taux_upper       NUMERIC(5,3),   -- intervalle de confiance haut (80%)
    modele           VARCHAR(50) DEFAULT 'Prophet',
    run_date         DATE NOT NULL,  -- date d'exécution du modèle
    UNIQUE (date_prediction, duree_ans, modele)
);

-- ── pipeline_log ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_log (
    id         SERIAL PRIMARY KEY,
    dag_id     VARCHAR(100) NOT NULL,
    source     VARCHAR(100) NOT NULL,
    type       VARCHAR(50) NOT NULL,
    status     VARCHAR(20) NOT NULL,
    records    INTEGER DEFAULT 0,
    message    TEXT,
    duration_s NUMERIC(10,2),
    ran_at     TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- VUES ANALYTIQUES
-- ============================================================

-- Prix les plus récents par commune
CREATE OR REPLACE VIEW v_prix_recents AS
SELECT DISTINCT ON (p.code_insee, p.type_bien)
    p.code_insee,
    c.nom,
    c.departement,
    c.region,
    c.population,
    c.latitude,
    c.longitude,
    p.type_bien,
    p.annee,
    p.prix_m2_median,
    p.prix_m2_moyen,
    p.nb_transactions
FROM prix_immobilier p
JOIN communes c USING (code_insee)
WHERE p.trimestre IS NULL
ORDER BY p.code_insee, p.type_bien, p.annee DESC;

-- Agrégation par département
CREATE OR REPLACE VIEW v_prix_departement AS
SELECT
    c.departement,
    c.region,
    p.type_bien,
    p.annee,
    COUNT(DISTINCT p.code_insee)              AS nb_communes,
    ROUND(AVG(p.prix_m2_median)::numeric, 0)  AS prix_m2_median_moyen,
    ROUND(MIN(p.prix_m2_median)::numeric, 0)  AS prix_m2_min,
    ROUND(MAX(p.prix_m2_median)::numeric, 0)  AS prix_m2_max,
    SUM(p.nb_transactions)                    AS total_transactions
FROM prix_immobilier p
JOIN communes c USING (code_insee)
WHERE p.trimestre IS NULL AND p.prix_m2_median IS NOT NULL
GROUP BY c.departement, c.region, p.type_bien, p.annee;

-- Taux actuels + OAT
CREATE OR REPLACE VIEW v_taux_avec_oat AS
SELECT
    t.date_obs,
    t.duree_ans,
    ROUND(AVG(t.taux_moyen)::numeric, 3)  AS taux_moyen,
    ROUND(MIN(t.taux_min)::numeric, 3)    AS taux_min,
    ROUND(MAX(t.taux_max)::numeric, 3)    AS taux_max,
    o.valeur                              AS oat_10ans,
    ROUND((AVG(t.taux_moyen) - o.valeur)::numeric, 3) AS spread_oat
FROM taux_nationaux t
LEFT JOIN oat_historique o
    ON DATE_TRUNC('month', o.date_obs) = DATE_TRUNC('month', t.date_obs)
GROUP BY t.date_obs, t.duree_ans, o.valeur
ORDER BY t.date_obs DESC, t.duree_ans;

-- Prédictions + historique (pour Power BI)
CREATE OR REPLACE VIEW v_taux_et_predictions AS
SELECT
    date_obs        AS date_obs,
    duree_ans,
    taux_moyen      AS valeur,
    taux_min        AS valeur_lower,
    taux_max        AS valeur_upper,
    'historique'    AS type
FROM taux_nationaux
UNION ALL
SELECT
    date_prediction,
    duree_ans,
    taux_predit,
    taux_lower,
    taux_upper,
    'prediction'
FROM predictions_taux
ORDER BY date_obs, duree_ans;

-- Fraîcheur des données (dashboard de monitoring)
CREATE OR REPLACE VIEW v_pipeline_status AS
SELECT 'DVF 2025'    AS source, MAX(loaded_at) AS derniere_maj,
       COUNT(DISTINCT code_insee) AS communes, SUM(nb_transactions) AS transactions
FROM prix_immobilier WHERE annee = 2025 AND source = 'DVF'
UNION ALL
SELECT 'DVF 2024',   MAX(loaded_at), COUNT(DISTINCT code_insee), SUM(nb_transactions)
FROM prix_immobilier WHERE annee = 2024 AND source = 'DVF'
UNION ALL
SELECT 'OAT',        MAX(updated_at), NULL, COUNT(*)
FROM oat_historique
UNION ALL
SELECT 'Taux crédit',MAX(scraped_at), NULL, COUNT(*)
FROM taux_nationaux;

-- ── Données seed taux ─────────────────────────────────────────
INSERT INTO communes (code_insee, nom, departement, region, population, latitude, longitude, geom)
VALUES
    ('75056','Paris','75','Île-de-France',2161000,48.8566,2.3522,ST_SetSRID(ST_MakePoint(2.3522,48.8566),4326)),
    ('69123','Lyon','69','Auvergne-Rhône-Alpes',522969,45.7640,4.8357,ST_SetSRID(ST_MakePoint(4.8357,45.7640),4326)),
    ('13055','Marseille','13','Provence-Alpes-Côte d''Azur',870731,43.2965,5.3698,ST_SetSRID(ST_MakePoint(5.3698,43.2965),4326))
ON CONFLICT DO NOTHING;

INSERT INTO taux_nationaux (date_obs, duree_ans, taux_moyen, taux_min, taux_max, taux_usure, source)
VALUES
    ('2026-05-01',10,2.990,2.700,3.200,5.19,'CAFPI'),
    ('2026-05-01',15,3.200,2.900,3.450,5.19,'CAFPI'),
    ('2026-05-01',20,3.330,3.050,3.550,5.19,'CAFPI'),
    ('2026-05-01',25,3.430,3.200,3.650,5.19,'CAFPI'),
    ('2026-04-01',15,3.130,2.800,3.400,5.22,'CAFPI'),
    ('2026-04-01',20,3.260,3.000,3.490,5.22,'CAFPI'),
    ('2026-03-01',15,3.130,2.800,3.380,5.25,'CAFPI'),
    ('2026-03-01',20,3.260,3.000,3.470,5.25,'CAFPI'),
    ('2026-02-01',15,3.180,2.850,3.420,5.27,'CAFPI'),
    ('2026-02-01',20,3.290,3.050,3.500,5.27,'CAFPI'),
    ('2026-01-01',15,3.200,2.900,3.450,5.30,'CAFPI'),
    ('2026-01-01',20,3.320,3.080,3.520,5.30,'CAFPI'),
    ('2025-12-01',15,3.250,2.950,3.500,5.32,'CAFPI'),
    ('2025-12-01',20,3.380,3.100,3.580,5.32,'CAFPI'),
    ('2025-11-01',15,3.280,2.980,3.520,5.34,'CAFPI'),
    ('2025-11-01',20,3.400,3.120,3.600,5.34,'CAFPI'),
    ('2025-10-01',15,3.300,3.000,3.550,5.36,'CAFPI'),
    ('2025-10-01',20,3.420,3.150,3.620,5.36,'CAFPI'),
    ('2025-09-01',15,3.320,3.020,3.570,5.38,'CAFPI'),
    ('2025-09-01',20,3.440,3.170,3.640,5.38,'CAFPI'),
    ('2025-06-01',15,3.450,3.100,3.700,5.42,'CAFPI'),
    ('2025-06-01',20,3.560,3.250,3.800,5.42,'CAFPI'),
    ('2025-03-01',15,3.600,3.250,3.850,5.48,'CAFPI'),
    ('2025-03-01',20,3.720,3.400,3.950,5.48,'CAFPI'),
    ('2024-12-01',15,3.750,3.400,4.000,5.80,'CAFPI'),
    ('2024-12-01',20,3.850,3.500,4.100,5.80,'CAFPI'),
    ('2024-06-01',15,3.900,3.550,4.150,6.00,'CAFPI'),
    ('2024-06-01',20,4.000,3.650,4.250,6.00,'CAFPI'),
    ('2024-01-01',15,4.150,3.800,4.400,6.29,'CAFPI'),
    ('2024-01-01',20,4.250,3.900,4.500,6.29,'CAFPI'),
    ('2023-06-01',15,3.750,3.400,4.000,5.80,'CAFPI'),
    ('2023-06-01',20,3.850,3.500,4.100,5.80,'CAFPI'),
    ('2023-01-01',15,2.800,2.500,3.050,4.50,'CAFPI'),
    ('2023-01-01',20,2.900,2.600,3.150,4.50,'CAFPI'),
    ('2022-06-01',15,1.800,1.500,2.050,2.80,'CAFPI'),
    ('2022-06-01',20,1.900,1.600,2.150,2.80,'CAFPI'),
    ('2022-01-01',15,1.050,0.850,1.250,2.40,'CAFPI'),
    ('2022-01-01',20,1.150,0.950,1.350,2.40,'CAFPI'),
    ('2021-01-01',15,1.000,0.800,1.200,2.40,'CAFPI'),
    ('2021-01-01',20,1.100,0.900,1.300,2.40,'CAFPI')
ON CONFLICT DO NOTHING;

-- Données OAT seed (source : Banque de France publique)
INSERT INTO oat_historique (date_obs, valeur, source)
VALUES
    ('2026-05-01', 3.750, 'BdF_seed'),
    ('2026-04-01', 3.500, 'BdF_seed'),
    ('2026-03-01', 3.200, 'BdF_seed'),
    ('2026-02-01', 3.100, 'BdF_seed'),
    ('2026-01-01', 3.050, 'BdF_seed'),
    ('2025-12-01', 3.000, 'BdF_seed'),
    ('2025-09-01', 2.900, 'BdF_seed'),
    ('2025-06-01', 3.100, 'BdF_seed'),
    ('2025-03-01', 3.300, 'BdF_seed'),
    ('2025-01-01', 3.200, 'BdF_seed'),
    ('2024-12-01', 3.100, 'BdF_seed'),
    ('2024-06-01', 3.200, 'BdF_seed'),
    ('2024-01-01', 2.800, 'BdF_seed'),
    ('2023-10-01', 3.500, 'BdF_seed'),
    ('2023-06-01', 3.000, 'BdF_seed'),
    ('2023-01-01', 2.500, 'BdF_seed'),
    ('2022-12-01', 2.400, 'BdF_seed'),
    ('2022-06-01', 1.900, 'BdF_seed'),
    ('2022-01-01', 0.300, 'BdF_seed'),
    ('2021-01-01',-0.340, 'BdF_seed'),
    ('2020-01-01',-0.040, 'BdF_seed')
ON CONFLICT DO NOTHING;
