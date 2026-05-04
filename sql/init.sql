-- ============================================================
-- immo-analytics — Schéma base de données
-- PostgreSQL 15 + PostGIS
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── Table : communes ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS communes (
    code_insee              VARCHAR(10) PRIMARY KEY,
    nom                     VARCHAR(200) NOT NULL,
    departement             VARCHAR(3),
    region                  VARCHAR(100),
    population              INTEGER,
    superficie_km2          NUMERIC(10,2),
    latitude                NUMERIC(9,6),
    longitude               NUMERIC(9,6),
    geom                    GEOMETRY(Point, 4326),
    revenu_median_annuel    NUMERIC(10,2),
    revenu_annee_ref        INTEGER,
    taux_chomage            NUMERIC(5,2),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_communes_nom_trgm ON communes USING GIN (nom gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_communes_geom     ON communes USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_communes_dept     ON communes (departement);
CREATE INDEX IF NOT EXISTS idx_communes_region   ON communes (region);

-- ── Table : prix_immobilier ───────────────────────────────────
CREATE TABLE IF NOT EXISTS prix_immobilier (
    id              SERIAL PRIMARY KEY,
    code_insee      VARCHAR(10) NOT NULL REFERENCES communes(code_insee) ON DELETE CASCADE,
    annee           SMALLINT NOT NULL,
    trimestre       SMALLINT,
    type_bien       VARCHAR(20) NOT NULL,   -- 'appartement', 'maison'
    prix_m2_moyen   NUMERIC(10,2),
    prix_m2_median  NUMERIC(10,2),
    nb_transactions INTEGER,
    surface_moyenne NUMERIC(8,2),
    prix_moyen      NUMERIC(12,2),
    source          VARCHAR(50) DEFAULT 'DVF',
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (code_insee, annee, trimestre, type_bien)
);

CREATE INDEX IF NOT EXISTS idx_prix_commune ON prix_immobilier (code_insee);
CREATE INDEX IF NOT EXISTS idx_prix_annee   ON prix_immobilier (annee);
CREATE INDEX IF NOT EXISTS idx_prix_type    ON prix_immobilier (type_bien);
CREATE INDEX IF NOT EXISTS idx_prix_source  ON prix_immobilier (source);

-- ── Table : taux_nationaux ────────────────────────────────────
CREATE TABLE IF NOT EXISTS taux_nationaux (
    id          SERIAL PRIMARY KEY,
    date_obs    DATE NOT NULL,
    duree_ans   SMALLINT NOT NULL,
    taux_moyen  NUMERIC(5,3) NOT NULL,
    taux_min    NUMERIC(5,3),
    taux_max    NUMERIC(5,3),
    taux_usure  NUMERIC(5,3),
    oat_10ans   NUMERIC(5,3),
    source      VARCHAR(100) NOT NULL,
    note        TEXT,
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (date_obs, duree_ans, source)
);

CREATE INDEX IF NOT EXISTS idx_taux_date  ON taux_nationaux (date_obs DESC);
CREATE INDEX IF NOT EXISTS idx_taux_duree ON taux_nationaux (duree_ans);

-- ── Table : taux_regionaux ────────────────────────────────────
CREATE TABLE IF NOT EXISTS taux_regionaux (
    id          SERIAL PRIMARY KEY,
    date_obs    DATE NOT NULL,
    region      VARCHAR(100) NOT NULL,
    duree_ans   SMALLINT NOT NULL,
    taux_moyen  NUMERIC(5,3),
    taux_min    NUMERIC(5,3),
    source      VARCHAR(100) DEFAULT 'Empruntis',
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (date_obs, region, duree_ans, source)
);

-- ── Table : pipeline_log ──────────────────────────────────────
-- Traçabilité de chaque exécution de DAG Airflow
CREATE TABLE IF NOT EXISTS pipeline_log (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100) NOT NULL,
    run_id          VARCHAR(200),
    source          VARCHAR(100) NOT NULL,
    type            VARCHAR(50) NOT NULL,   -- 'dvf', 'taux', 'insee', 'geo'
    status          VARCHAR(20) NOT NULL,   -- 'success', 'error', 'partial'
    records         INTEGER DEFAULT 0,
    message         TEXT,
    duration_s      NUMERIC(10,2),
    ran_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_log_dag    ON pipeline_log (dag_id);
CREATE INDEX IF NOT EXISTS idx_log_ran_at ON pipeline_log (ran_at DESC);

-- ============================================================
-- VUES ANALYTIQUES
-- ============================================================

-- Vue : prix les plus récents par commune
CREATE OR REPLACE VIEW v_prix_recents AS
SELECT DISTINCT ON (p.code_insee, p.type_bien)
    p.code_insee,
    c.nom,
    c.departement,
    c.region,
    c.population,
    c.latitude,
    c.longitude,
    c.revenu_median_annuel,
    c.revenu_annee_ref,
    p.type_bien,
    p.annee,
    p.prix_m2_median,
    p.prix_m2_moyen,
    p.nb_transactions,
    -- Ratio effort d'achat : prix 50m² / revenu annuel
    CASE
        WHEN c.revenu_median_annuel > 0 AND p.prix_m2_median > 0
        THEN ROUND((p.prix_m2_median * 50 / c.revenu_median_annuel)::numeric, 1)
        ELSE NULL
    END AS ratio_effort_achat
FROM prix_immobilier p
JOIN communes c USING (code_insee)
WHERE p.trimestre IS NULL
ORDER BY p.code_insee, p.type_bien, p.annee DESC;

-- Vue : prix par département (agrégation)
CREATE OR REPLACE VIEW v_prix_departement AS
SELECT
    c.departement,
    c.region,
    p.type_bien,
    p.annee,
    COUNT(DISTINCT p.code_insee)            AS nb_communes,
    ROUND(AVG(p.prix_m2_median)::numeric, 0) AS prix_m2_median_moyen,
    ROUND(MIN(p.prix_m2_median)::numeric, 0) AS prix_m2_min,
    ROUND(MAX(p.prix_m2_median)::numeric, 0) AS prix_m2_max,
    SUM(p.nb_transactions)                   AS total_transactions
FROM prix_immobilier p
JOIN communes c USING (code_insee)
WHERE p.trimestre IS NULL
  AND p.prix_m2_median IS NOT NULL
GROUP BY c.departement, c.region, p.type_bien, p.annee;

-- Vue : prix par région (agrégation)
CREATE OR REPLACE VIEW v_prix_region AS
SELECT
    c.region,
    p.type_bien,
    p.annee,
    COUNT(DISTINCT p.code_insee)              AS nb_communes,
    ROUND(AVG(p.prix_m2_median)::numeric, 0)  AS prix_m2_median_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.prix_m2_median)::numeric, 0)
                                               AS prix_m2_median_reel,
    ROUND(MIN(p.prix_m2_median)::numeric, 0)  AS prix_m2_min,
    ROUND(MAX(p.prix_m2_median)::numeric, 0)  AS prix_m2_max,
    SUM(p.nb_transactions)                    AS total_transactions
FROM prix_immobilier p
JOIN communes c USING (code_insee)
WHERE p.trimestre IS NULL
  AND p.prix_m2_median IS NOT NULL
GROUP BY c.region, p.type_bien, p.annee;

-- Vue : évolution des taux (derniers disponibles par durée)
CREATE OR REPLACE VIEW v_taux_actuels AS
SELECT DISTINCT ON (duree_ans)
    date_obs,
    duree_ans,
    ROUND(AVG(taux_moyen) OVER (PARTITION BY date_obs, duree_ans)::numeric, 3) AS taux_moyen,
    MIN(taux_min)    OVER (PARTITION BY date_obs, duree_ans)                    AS taux_min,
    MAX(taux_max)    OVER (PARTITION BY date_obs, duree_ans)                    AS taux_max,
    taux_usure,
    oat_10ans,
    STRING_AGG(source, ', ') OVER (PARTITION BY date_obs, duree_ans)           AS sources
FROM taux_nationaux
ORDER BY duree_ans, date_obs DESC;

-- Vue : top communes les plus chères par région
CREATE OR REPLACE VIEW v_top_communes_par_region AS
SELECT *
FROM (
    SELECT
        code_insee,
        nom,
        departement,
        region,
        type_bien,
        annee,
        prix_m2_median,
        nb_transactions,
        population,
        ROW_NUMBER() OVER (
            PARTITION BY region, type_bien, annee
            ORDER BY prix_m2_median DESC
        ) AS rang
    FROM v_prix_recents
    WHERE nb_transactions >= 10
) ranked
WHERE rang <= 10;

-- Vue : tableau de bord pipeline (fraîcheur des données)
CREATE OR REPLACE VIEW v_pipeline_status AS
SELECT
    'DVF 2025'      AS source,
    MAX(loaded_at)  AS derniere_maj,
    COUNT(DISTINCT code_insee) AS nb_communes,
    SUM(nb_transactions) AS nb_transactions
FROM prix_immobilier WHERE annee = 2025 AND source = 'DVF'
UNION ALL
SELECT
    'DVF 2024',
    MAX(loaded_at),
    COUNT(DISTINCT code_insee),
    SUM(nb_transactions)
FROM prix_immobilier WHERE annee = 2024 AND source = 'DVF'
UNION ALL
SELECT
    'Taux courtiers',
    MAX(scraped_at),
    NULL,
    COUNT(*)
FROM taux_nationaux;

-- ── Données seed taux ─────────────────────────────────────────
INSERT INTO communes (code_insee, nom, departement, region, population, latitude, longitude, geom)
VALUES
    ('75056','Paris','75','Île-de-France',2161000,48.8566,2.3522,ST_SetSRID(ST_MakePoint(2.3522,48.8566),4326)),
    ('69123','Lyon','69','Auvergne-Rhône-Alpes',522969,45.7640,4.8357,ST_SetSRID(ST_MakePoint(4.8357,45.7640),4326)),
    ('13055','Marseille','13','Provence-Alpes-Côte d''Azur',870731,43.2965,5.3698,ST_SetSRID(ST_MakePoint(5.3698,43.2965),4326))
ON CONFLICT DO NOTHING;

INSERT INTO taux_nationaux (date_obs, duree_ans, taux_moyen, taux_min, taux_max, taux_usure, oat_10ans, source, note)
VALUES
    ('2026-05-01',10,2.990,2.700,3.200,5.19,3.75,'CAFPI','Seed mai 2026'),
    ('2026-05-01',15,3.200,2.900,3.450,5.19,3.75,'CAFPI','Seed mai 2026'),
    ('2026-05-01',20,3.330,3.050,3.550,5.19,3.75,'CAFPI','Seed mai 2026'),
    ('2026-05-01',25,3.430,3.200,3.650,5.19,3.75,'CAFPI','Seed mai 2026'),
    ('2026-04-01',15,3.130,2.800,3.400,5.22,3.50,'CAFPI','Seed avril 2026'),
    ('2026-04-01',20,3.260,3.000,3.490,5.22,3.50,'CAFPI','Seed avril 2026'),
    ('2026-03-01',15,3.130,2.800,3.380,5.25,3.20,'CAFPI','Seed mars 2026'),
    ('2026-03-01',20,3.260,3.000,3.470,5.25,3.20,'CAFPI','Seed mars 2026'),
    ('2026-02-01',15,3.180,2.850,3.420,5.27,3.10,'CAFPI','Seed février 2026'),
    ('2026-02-01',20,3.290,3.050,3.500,5.27,3.10,'CAFPI','Seed février 2026'),
    ('2026-01-01',15,3.200,2.900,3.450,5.30,3.05,'CAFPI','Seed janvier 2026'),
    ('2026-01-01',20,3.320,3.080,3.520,5.30,3.05,'CAFPI','Seed janvier 2026'),
    ('2025-12-01',15,3.250,2.950,3.500,5.32,3.00,'CAFPI','Seed décembre 2025'),
    ('2025-12-01',20,3.380,3.100,3.580,5.32,3.00,'CAFPI','Seed décembre 2025')
ON CONFLICT DO NOTHING;
