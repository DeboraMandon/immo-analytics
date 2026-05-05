"""
DAG : dvf_loader
Chargement DVF (Demandes de Valeurs Foncières)

Orchestration uniquement — logique métier dans ingestion/dvf_loader.py

Schedule : 1er avril et 1er octobre (publications semestrielles DGFiP)
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPT = "/opt/airflow/ingestion/dvf_loader.py"
DB_URL = os.environ.get("IMMO_DB_URL", "")
ANNEE  = os.environ.get("DVF_ANNEE", "2025")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def _run(args: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, SCRIPT] + args,
        capture_output=True, text=True,
        env={**os.environ, "DATABASE_URL": DB_URL},
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[-2000:])
        raise Exception(f"Script failed (exit {result.returncode})")


def check_update(**context):
    import json
    import urllib.request
    url = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres-geolocalisees/"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            meta   = json.loads(r.read())
        annee  = int(meta.get("last_modified", "2026")[:4]) - 1
    except Exception:
        annee  = int(ANNEE)
    print(f"Année DVF cible : {annee}")
    context["task_instance"].xcom_push(key="annee", value=annee)


def enrich_geo(**context):
    annee = context["task_instance"].xcom_pull(key="annee") or ANNEE
    _run(["--all-france", f"--annee={annee}", "--skip-geo"])


def load_dvf(**context):
    annee = context["task_instance"].xcom_pull(key="annee") or ANNEE
    _run(["--all-france", f"--annee={annee}"])


with DAG(
    dag_id="dvf_loader",
    description="Chargement DVF semestriel depuis data.gouv.fr",
    schedule="0 3 1 4,10 *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "prix", "ingestion"],
) as dag:

    t1 = PythonOperator(task_id="check_update",  python_callable=check_update)
    t2 = PythonOperator(task_id="enrich_geo",    python_callable=enrich_geo,
                        execution_timeout=timedelta(minutes=15))
    t3 = PythonOperator(task_id="load_dvf",      python_callable=load_dvf,
                        execution_timeout=timedelta(hours=2))

    t1 >> t2 >> t3
