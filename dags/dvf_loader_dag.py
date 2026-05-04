"""
DAG : dvf_loader
Chargement DVF (Demandes de Valeurs Foncières) depuis data.gouv.fr

Principe d'architecture :
    Ce DAG orchestre UNIQUEMENT — toute la logique métier est dans
    ingestion/dvf_loader.py. Le DAG appelle ce script en subprocess.

Schedule : 1er avril et 1er octobre (publications semestrielles DGFiP)
Déclenchable manuellement depuis l'UI Airflow.
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Constantes ────────────────────────────────────────────────
SCRIPT = "/opt/airflow/ingestion/dvf_loader.py"
DB_URL = os.environ.get("IMMO_DB_URL", "")
DVF_ANNEE_DEFAULT = int(os.environ.get("DVF_ANNEE", "2025"))

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def _run_script(args: list[str]) -> None:
    """Lance dvf_loader.py avec les arguments donnés."""
    cmd = [sys.executable, SCRIPT] + args
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env={**os.environ, "DATABASE_URL": DB_URL},
    )
    # Toujours afficher stdout dans les logs Airflow
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[-2000:])
        raise Exception(f"Script failed (exit {result.returncode})")


def check_dvf_update(**context):
    """
    Vérifie si une nouvelle version DVF est disponible sur data.gouv.fr.
    Pousse l'année disponible dans XCom pour les tâches suivantes.
    """
    import json
    import urllib.request

    url = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres-geolocalisees/"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            meta = json.loads(r.read())
        last_modified = meta.get("last_modified", "")
        annee = int(last_modified[:4]) - 1 if last_modified else DVF_ANNEE_DEFAULT
    except Exception as e:
        print(f"Impossible de vérifier data.gouv.fr : {e} — utilisation année par défaut")
        annee = DVF_ANNEE_DEFAULT

    print(f"Année DVF cible : {annee}")
    context["task_instance"].xcom_push(key="annee_dvf", value=annee)


def enrich_geo(**context):
    """
    Enrichit les communes avec coordonnées GPS et population.
    Source : geo.api.gouv.fr
    Lance : dvf_loader.py --all-france --annee X --skip-geo
    (Le flag --skip-geo désactive le chargement DVF, garde uniquement le geo)
    """
    annee = context["task_instance"].xcom_pull(key="annee_dvf") or DVF_ANNEE_DEFAULT
    _run_script(["--all-france", f"--annee={annee}", "--skip-geo"])


def load_dvf(**context):
    """
    Charge les prix DVF pour toute la France.
    Lance : dvf_loader.py --all-france --annee X --skip-dvf
    (Le flag --skip-dvf désactive l'enrichissement geo, déjà fait)
    """
    annee = context["task_instance"].xcom_pull(key="annee_dvf") or DVF_ANNEE_DEFAULT
    _run_script(["--all-france", f"--annee={annee}", "--skip-dvf"])


# ── DAG ───────────────────────────────────────────────────────
with DAG(
    dag_id="dvf_loader",
    description="Chargement DVF depuis data.gouv.fr — orchestration uniquement",
    schedule="0 3 1 4,10 *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "ingestion"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(
        task_id="check_dvf_update",
        python_callable=check_dvf_update,
    )

    t2 = PythonOperator(
        task_id="enrich_geo",
        python_callable=enrich_geo,
        execution_timeout=timedelta(minutes=10),
    )

    t3 = PythonOperator(
        task_id="load_dvf",
        python_callable=load_dvf,
        execution_timeout=timedelta(hours=2),
    )

    t1 >> t2 >> t3
