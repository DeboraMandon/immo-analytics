# immo-analytics 🏠

**Pipeline data engineering — Marché immobilier français**

Stack 100% data engineering : PostgreSQL + Airflow + Metabase.
Zéro frontend à coder — les dashboards se construisent dans Metabase.

---

## Architecture

```
data.gouv.fr (DVF)  ──┐
geo.api.gouv.fr     ──┤
CAFPI / Pretto      ──┤──► Airflow DAGs ──► PostgreSQL ──► Metabase
INSEE Filosofi      ──┘                      + PostGIS     (dashboards)
```

## Stack

| Composant | Rôle | Port |
|---|---|---|
| PostgreSQL 15 + PostGIS | Stockage données | 5433 |
| Apache Airflow 2.9 | Orchestration pipelines | 8080 |
| Metabase | Dashboards analytiques | 3000 |

## Démarrage

```bash
git clone https://github.com/DeboraMandon/immo-analytics.git
cd immo-analytics
cp .env.example .env
# Editer .env avec vos valeurs
docker compose up -d
```

- **Airflow** : http://localhost:8080 (admin / admin)
- **Metabase** : http://localhost:3000

## DAGs Airflow

| DAG | Schedule | Source | Table |
|---|---|---|---|
| `dvf_loader` | 1er avril + 1er oct. | DVF DGFiP | `prix_immobilier` |
| `taux_scraper` | Quotidien 6h | CAFPI, Pretto | `taux_nationaux` |
| `insee_loader` | 1er janvier | INSEE Filosofi | `communes` |

## Schéma base de données

```sql
communes          -- 33k communes françaises + coordonnées GPS
prix_immobilier   -- Prix m² DVF par commune, type, année
taux_nationaux    -- Taux crédit immobilier historiques
taux_regionaux    -- Taux par région
pipeline_log      -- Traçabilité des exécutions
```

## Vues analytiques disponibles

```sql
v_prix_recents          -- Prix les plus récents par commune
v_prix_departement      -- Agrégation par département
v_prix_region           -- Agrégation par région
v_taux_actuels          -- Derniers taux par durée
v_top_communes_par_region  -- Top 10 communes par région
v_pipeline_status       -- Fraîcheur des données
```

## Migration depuis immo-dashboard

Si vous avez déjà chargé des données dans l'ancien projet :

```bash
# Export depuis l'ancien postgres (port 5432 ou 5433)
docker exec immo_postgres pg_dump -U immo_user immo_dashboard \
  -t communes -t prix_immobilier -t taux_nationaux \
  > backup_immo.sql

# Import dans le nouveau postgres
docker compose exec -T postgres psql -U immo_user immo_dashboard < backup_immo.sql
```

## Licence

MIT — Données : DVF (Licence Ouverte Etalab) · INSEE (Licence Ouverte v2.0)
