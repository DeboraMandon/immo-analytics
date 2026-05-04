"""
DAG : taux_scraper
Scraping des taux de crédit immobilier depuis les courtiers publics

Principe d'architecture :
    Ce DAG orchestre UNIQUEMENT — toute la logique métier est dans
    ingestion/taux_scraper.py. Le DAG appelle ce script en subprocess.

Schedule : quotidien à 6h
Sources   : CAFPI, Pretto (pages publiques)
Table     : taux_nationaux
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPT = "/opt/airflow/ingestion/taux_scraper.py"
DB_URL = os.environ.get("IMMO_DB_URL", "")

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}


def scrape_taux(**context):
    """Lance taux_scraper.py en subprocess."""
    result = subprocess.run(
        [sys.executable, SCRIPT],
        capture_output=True,
        text=True,
        env={**os.environ, "DATABASE_URL": DB_URL},
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[-2000:])
        raise Exception(f"taux_scraper failed (exit {result.returncode})")


with DAG(
    dag_id="taux_scraper",
    description="Scraping taux crédit immobilier (CAFPI, Pretto) — orchestration uniquement",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["taux", "scraping", "immobilier"],
    doc_md=__doc__,
) as dag:

    PythonOperator(
        task_id="scrape_taux",
        python_callable=scrape_taux,
        execution_timeout=timedelta(minutes=30),
    )
