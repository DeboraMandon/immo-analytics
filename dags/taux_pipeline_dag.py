"""
DAG : taux_scraper
Scraping taux crédit immobilier + chargement OAT Banque de France
+ prédiction Prophet 6 mois

Orchestration uniquement — logique dans ingestion/

Schedule : 1er du mois à 6h
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DB_URL = os.environ.get("IMMO_DB_URL", "")

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}


def _run(script: str, args: list[str] = None) -> None:
    result = subprocess.run(
        [sys.executable, f"/opt/airflow/ingestion/{script}"] + (args or []),
        capture_output=True, text=True,
        env={**os.environ, "DATABASE_URL": DB_URL},
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[-2000:])
        raise Exception(f"{script} failed (exit {result.returncode})")


def scrape_taux(**context):
    _run("taux_scraper.py")


def load_oat(**context):
    _run("oat_loader.py")


def predict(**context):
    _run("predict_taux.py", ["--all-durees", "--horizon=6"])


with DAG(
    dag_id="taux_pipeline",
    description="Taux crédit + OAT Banque de France + prédiction 6 mois",
    schedule="0 6 1 * *",   # 1er du mois à 6h
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["taux", "oat", "prediction"],
) as dag:

    t1 = PythonOperator(task_id="scrape_taux", python_callable=scrape_taux,
                        execution_timeout=timedelta(minutes=15))
    t2 = PythonOperator(task_id="load_oat",    python_callable=load_oat,
                        execution_timeout=timedelta(minutes=10))
    t3 = PythonOperator(task_id="predict_taux",python_callable=predict,
                        execution_timeout=timedelta(minutes=30))

    [t1, t2] >> t3
