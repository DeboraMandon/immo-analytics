"""
DAG : insee_loader
Chargement des revenus médians INSEE (Filosofi) par commune

Principe d'architecture :
    Ce DAG orchestre UNIQUEMENT — toute la logique métier est dans
    ingestion/insee_loader.py. Le DAG appelle ce script en subprocess.

Schedule  : annuel (1er janvier)
Source    : API INSEE Filosofi (token gratuit requis)
Table     : communes.revenu_median_annuel
Prérequis : INSEE_API_TOKEN dans .env
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPT = "/opt/airflow/ingestion/insee_loader.py"
DB_URL = os.environ.get("IMMO_DB_URL", "")
INSEE_TOKEN = os.environ.get("INSEE_API_TOKEN", "")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(hours=1),
    "email_on_failure": False,
}


def check_token(**context):
    """Vérifie que le token INSEE est configuré avant de lancer le script."""
    if not INSEE_TOKEN:
        raise ValueError(
            "INSEE_API_TOKEN non configuré. "
            "Créer un compte gratuit sur https://api.insee.fr/ "
            "puis ajouter le token dans .env"
        )
    print("Token INSEE présent")


def load_revenus(**context):
    """Lance insee_loader.py en subprocess."""
    result = subprocess.run(
        [sys.executable, SCRIPT],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "DATABASE_URL": DB_URL,
            "INSEE_API_TOKEN": INSEE_TOKEN,
        },
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[-2000:])
        raise Exception(f"insee_loader failed (exit {result.returncode})")


with DAG(
    dag_id="insee_loader",
    description="Chargement revenus médians INSEE par commune — orchestration uniquement",
    schedule="0 4 1 1 *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["insee", "revenus", "ingestion"],
    doc_md=__doc__,
) as dag:

    t1 = PythonOperator(
        task_id="check_token",
        python_callable=check_token,
    )

    t2 = PythonOperator(
        task_id="load_revenus",
        python_callable=load_revenus,
        execution_timeout=timedelta(hours=4),
    )

    t1 >> t2
