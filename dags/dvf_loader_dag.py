"""
DAG : dvf_loader
Charge les données DVF (Demandes de Valeurs Foncières) depuis data.gouv.fr

Schedule :
  - Semestriel : avril et octobre (publications DGFiP)
  - Déclenchable manuellement depuis l'UI Airflow

Source : https://files.data.gouv.fr/geo-dvf/latest/
"""
from datetime import datetime, timedelta
import subprocess
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ── Config ────────────────────────────────────────────────────
IMMO_DB_URL = os.environ.get("IMMO_DB_URL", "")
DVF_ANNEE   = int(os.environ.get("DVF_ANNEE", "2025"))

ALL_DEPARTEMENTS = [
    "01","02","03","04","05","06","07","08","09","10",
    "11","12","13","14","15","16","17","18","19","21",
    "22","23","24","25","26","27","28","29","2A","2B",
    "30","31","32","33","34","35","36","37","38","39",
    "40","41","42","43","44","45","46","47","48","49",
    "50","51","52","53","54","55","56","58","59","60",
    "61","62","63","64","65","66","69","70","71","72",
    "73","74","75","76","77","78","79","80","81","82",
    "83","84","85","86","87","88","89","90","91","92",
    "93","94","95","971","972","973","974",
]

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def check_dvf_update(**context):
    """
    Vérifie si une nouvelle version DVF est disponible sur data.gouv.fr.
    Si non → skip le reste du DAG.
    """
    import urllib.request
    import json

    url = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres-geolocalisees/"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            meta = json.loads(r.read())
        last_modified = meta.get("last_modified", "")
        annee_dispo = int(last_modified[:4]) - 1 if last_modified else DVF_ANNEE
        context["task_instance"].xcom_push(key="annee_dvf", value=annee_dispo)
        print(f"DVF disponible : {annee_dispo}")
        return annee_dispo
    except Exception as e:
        print(f"Impossible de vérifier DVF update: {e}")
        return DVF_ANNEE


def enrich_geo(**context):
    """Enrichit les communes avec coordonnées GPS depuis geo.api.gouv.fr."""
    sys.path.insert(0, "/opt/airflow/ingestion")
    import asyncio
    import asyncpg
    import json
    import urllib.request

    async def run():
        conn = await asyncpg.connect(IMMO_DB_URL)
        enriched = 0

        for dept in ALL_DEPARTEMENTS:
            url = (
                f"https://geo.api.gouv.fr/communes"
                f"?codeDepartement={dept}"
                f"&fields=code,nom,population,centre,region"
                f"&format=json"
            )
            try:
                with urllib.request.urlopen(url, timeout=20) as r:
                    communes = json.loads(r.read())
            except Exception as e:
                print(f"Geo dept {dept}: {e}")
                continue

            for c in communes:
                coords = c.get("centre", {}).get("coordinates", [None, None])
                lng, lat = coords[0], coords[1]
                try:
                    await conn.execute("""
                        INSERT INTO communes
                            (code_insee, nom, departement, population, latitude, longitude, geom)
                        VALUES ($1, $2, $3, $4, $5::numeric, $6::numeric,
                            ST_SetSRID(ST_MakePoint($6::float8, $5::float8), 4326))
                        ON CONFLICT (code_insee) DO UPDATE SET
                            nom        = EXCLUDED.nom,
                            population = COALESCE(EXCLUDED.population, communes.population),
                            latitude   = COALESCE(EXCLUDED.latitude,   communes.latitude),
                            longitude  = COALESCE(EXCLUDED.longitude,  communes.longitude),
                            geom       = COALESCE(EXCLUDED.geom,       communes.geom),
                            updated_at = NOW()
                    """, c["code"], c["nom"], dept,
                         c.get("population"), lat, lng)
                    enriched += 1
                except Exception as e:
                    print(f"  Insert {c['code']}: {e}")

        await conn.close()
        print(f"Geo enrichissement terminé : {enriched} communes")
        return enriched

    return asyncio.run(run())


def load_dvf(**context):
    """Charge les données DVF pour tous les départements."""
    annee = context["task_instance"].xcom_pull(key="annee_dvf") or DVF_ANNEE
    sys.path.insert(0, "/opt/airflow/ingestion")

    # Lance dvf_loader.py en subprocess pour isolation mémoire
    result = subprocess.run(
        [
            sys.executable,
            "/opt/airflow/ingestion/dvf_loader.py",
            "--all-france",
            f"--annee={annee}",
            "--skip-geo",  # geo déjà fait dans la tâche précédente
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "DATABASE_URL": IMMO_DB_URL},
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dvf_loader failed (code {result.returncode})")


def log_pipeline(status, records, message, **context):
    """Log le résultat du pipeline dans pipeline_log."""
    import asyncio
    import asyncpg

    async def run():
        conn = await asyncpg.connect(IMMO_DB_URL)
        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, run_id, source, type, status, records, message)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
            context["dag"].dag_id,
            context["run_id"],
            "DVF DGFiP",
            "dvf",
            status,
            records,
            message,
        )
        await conn.close()

    asyncio.run(run())


# ── DAG définition ────────────────────────────────────────────
with DAG(
    dag_id="dvf_loader",
    description="Chargement DVF (Demandes de Valeurs Foncières) depuis data.gouv.fr",
    schedule="0 3 1 4,10 *",   # 1er avril et 1er octobre à 3h
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "ingestion"],
    doc_md="""
## DVF Loader DAG

Charge les données DVF depuis data.gouv.fr.

**Schedule :** 1er avril et 1er octobre (publications semestrielles DGFiP)

**Sources :**
- DVF : https://files.data.gouv.fr/geo-dvf/latest/
- Géo : https://geo.api.gouv.fr/communes

**Tables alimentées :**
- `communes` (coordonnées GPS, population)
- `prix_immobilier` (prix médian m² par commune, type de bien, année)
    """,
) as dag:

    t1 = PythonOperator(
        task_id="check_dvf_update",
        python_callable=check_dvf_update,
        doc_md="Vérifie si une nouvelle version DVF est disponible sur data.gouv.fr",
    )

    t2 = PythonOperator(
        task_id="enrich_geo",
        python_callable=enrich_geo,
        doc_md="Enrichit les communes avec coordonnées GPS (geo.api.gouv.fr)",
    )

    t3 = PythonOperator(
        task_id="load_dvf",
        python_callable=load_dvf,
        doc_md="Télécharge et charge les CSV DVF commune par commune",
        execution_timeout=timedelta(hours=2),
    )

    t1 >> t2 >> t3
