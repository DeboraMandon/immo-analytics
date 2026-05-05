# immo-analytics 🏠

**Pipeline data engineering — Marché immobilier français**

Suivi des taux de crédit immobilier et des prix au m² par ville en France.
Architecture 100% data engineering : ingestion automatisée, orchestration Airflow, visualisation Power BI.

---

## Architecture

```
Sources de données          Pipeline              Stockage          Visualisation
──────────────────          ────────              ────────          ─────────────
DVF DGFiP              ──►                                          Power BI Desktop
geo.api.gouv.fr        ──►  Airflow DAGs    ──►  PostgreSQL 15      (.pbip versionné)
Banque de France (OAT) ──►  (orchestration)      + PostGIS
CAFPI (taux crédit)    ──►  ingestion/
                            (scripts Python)
```

---

## Stack technique

| Composant | Rôle | Port |
|---|---|---|
| PostgreSQL 15 + PostGIS | Stockage données géospatiales | 5433 |
| Apache Airflow 2.9 | Orchestration des pipelines | 8080 |
| Power BI Desktop | Dashboards et visualisation | — |
| Docker Compose | Infrastructure | — |

---

## Démarrage rapide

```bash
git clone https://github.com/DeboraMandon/immo-analytics.git
cd immo-analytics
cp .env.example .env
# Éditer .env : POSTGRES_PASSWORD, AIRFLOW_FERNET_KEY (voir .env.example)
docker compose up -d
```

**Interfaces :**
- Airflow : http://localhost:8080 (admin / admin)
- PostgreSQL : localhost:5433 (DBeaver, pgAdmin, Power BI)

**Chargement initial des données :**
```bash
# Géolocalisation + DVF 2025 (France entière, ~45 min)
docker compose run --rm ingestion python dvf_loader.py --all-france --annee 2025

# OAT Banque de France (historique depuis 2015)
docker compose run --rm ingestion python oat_loader.py --depuis 2015-01

# Taux crédit CAFPI
docker compose run --rm ingestion python taux_scraper.py

# Prédiction taux 6 mois (Prophet)
docker compose run --rm ingestion python predict_taux.py --all-durees
```

---

## DAGs Airflow

| DAG | Schedule | Sources | Tables alimentées |
|---|---|---|---|
| `dvf_loader` | 1er avril + 1er oct. | DVF DGFiP, geo.api.gouv.fr | `communes`, `prix_immobilier` |
| `taux_pipeline` | 1er du mois à 6h | CAFPI, Banque de France | `taux_nationaux`, `oat_historique`, `predictions_taux` |

---

## Schéma base de données

```sql
communes            -- 33k communes françaises, coordonnées GPS, population
prix_immobilier     -- Prix m² DVF par commune, type de bien, année (2024 + 2025)
taux_nationaux      -- Taux crédit immobilier mensuels (CAFPI)
oat_historique      -- OAT 10 ans Banque de France (référence officielle)
predictions_taux    -- Prédictions Prophet 6 mois avec intervalles de confiance
pipeline_log        -- Traçabilité de chaque exécution de pipeline
```

## Vues analytiques

```sql
v_prix_recents          -- Prix les plus récents par commune (DVF)
v_prix_departement      -- Agrégation prix par département et année
v_taux_avec_oat         -- Taux crédit + OAT + spread bancaire
v_taux_et_predictions   -- Historique + prédictions Prophet (pour Power BI)
v_pipeline_status       -- Fraîcheur des données par source
```

---

## Sources de données

| Source | Données | Licence | Fréquence |
|---|---|---|---|
| DVF DGFiP / data.gouv.fr | Prix ventes immobilières | Licence Ouverte Etalab | Semestrielle |
| geo.api.gouv.fr | Coordonnées GPS, population | Licence Ouverte | Stable |
| Banque de France | OAT 10 ans | Données publiques | Mensuelle |
| CAFPI | Taux crédit indicatifs | Page publique (scraping) | Mensuelle |

### Limites connues

- **DVF** ne couvre pas Haut-Rhin (68), Bas-Rhin (67), Moselle (57), Mayotte (976) — livre foncier local
- **Taux CAFPI** : indicatifs, hors assurance, profil standard — pas des taux par banque
- **Prédictions** : fiables sur 1-3 mois, incertaines au-delà de 6 mois
- **Revenus INSEE** : non chargés par défaut (token requis sur https://portail-api.insee.fr/)

---

## Structure du projet

```
immo-analytics/
├── docker-compose.yml          # Infrastructure Docker
├── .env.example                # Variables d'environnement (template)
├── dags/
│   ├── dvf_loader_dag.py       # DAG semestriel DVF
│   └── taux_pipeline_dag.py    # DAG mensuel taux + OAT + prédiction
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dvf_loader.py           # Chargement DVF (prix m²)
│   ├── oat_loader.py           # Chargement OAT Banque de France
│   ├── taux_scraper.py         # Scraping taux CAFPI
│   └── predict_taux.py         # Modèle Prophet 6 mois
├── sql/
│   ├── init.sql                # Schéma + vues + données seed
│   └── init_airflow.sql        # Base Airflow
├── powerbi/                    # Fichiers .pbip (Power BI)
└── .github/
    └── workflows/ci.yml        # Lint Python (ruff)
```

---

## Licence

MIT — Données : DVF (Licence Ouverte Etalab) · OAT (Banque de France) · INSEE (Licence Ouverte v2.0)
