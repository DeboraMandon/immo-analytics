"""
DAG : insee_loader
Charge les données socio-économiques INSEE par commune

Schedule : annuel (janvier) — les données ont ~18 mois de décalage

Sources :
  - API INSEE (token gratuit sur https://api.insee.fr/)
  - Base Filosofi : revenus médians par commune
  - Base RP : population par commune

Prérequis :
  - Variable Airflow "INSEE_API_TOKEN" ou env INSEE_API_TOKEN
"""
from datetime import datetime, timedelta
import os
import asyncio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

IMMO_DB_URL     = os.environ.get("IMMO_DB_URL", "")
INSEE_API_TOKEN = os.environ.get("INSEE_API_TOKEN", "")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(hours=1),
    "email_on_failure": False,
}


def check_token(**context):
    """Vérifie que le token INSEE est configuré."""
    token = INSEE_API_TOKEN or Variable.get("INSEE_API_TOKEN", default_var="")
    if not token:
        raise ValueError(
            "INSEE_API_TOKEN non configuré. "
            "Créer un compte gratuit sur https://api.insee.fr/ "
            "puis ajouter le token dans .env ou dans les Variables Airflow."
        )
    context["task_instance"].xcom_push(key="token", value=token)
    print("Token INSEE OK")


def load_revenus_filosofi(**context):
    """
    Charge les revenus médians par commune depuis la base Filosofi INSEE.

    Endpoint : https://api.insee.fr/donnees-locales/V0.1/donnees/
               geo-FILOSOFI_DISP_MED@FILOSOFI-2021/COM-{code}

    Note : données disponibles avec ~18 mois de décalage.
    """
    import httpx
    import asyncpg

    token = context["task_instance"].xcom_pull(key="token")

    HEADERS = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    # Année la plus récente disponible (2021 en mai 2026)
    ANNEE_REF = 2021

    async def fetch_commune_revenus(client, code_insee):
        """Récupère le revenu médian pour une commune."""
        url = (
            f"https://api.insee.fr/donnees-locales/V0.1/donnees/"
            f"geo-FILOSOFI_DISP_MED@FILOSOFI-{ANNEE_REF}/COM-{code_insee}"
        )
        try:
            resp = await client.get(url, timeout=10)
            if resp.status_code != 200:
                return None
            data = resp.json()
            # Parser la réponse INSEE (structure variable)
            valeur = (
                data.get("Cellule", [{}])[0]
                    .get("Valeur", None)
            )
            return float(valeur) if valeur else None
        except Exception:
            return None

    async def run():
        # Récupérer tous les codes communes en base
        conn = await asyncpg.connect(IMMO_DB_URL)
        codes = await conn.fetch("SELECT code_insee FROM communes WHERE latitude IS NOT NULL")
        await conn.close()

        print(f"{len(codes)} communes à enrichir")
        updated = 0

        async with httpx.AsyncClient(headers=HEADERS) as client:
            # Traitement par batch de 50 pour éviter le rate limiting
            batch_size = 50
            for i in range(0, len(codes), batch_size):
                batch = codes[i:i+batch_size]
                tasks = [fetch_commune_revenus(client, r["code_insee"]) for r in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                conn = await asyncpg.connect(IMMO_DB_URL)
                for j, result in enumerate(results):
                    if isinstance(result, float) and result > 0:
                        await conn.execute("""
                            UPDATE communes
                            SET revenu_median_annuel = $1,
                                revenu_annee_ref     = $2,
                                updated_at           = NOW()
                            WHERE code_insee = $3
                        """, result, ANNEE_REF, batch[j]["code_insee"])
                        updated += 1
                await conn.close()

                if i % 500 == 0:
                    print(f"  Progression : {i}/{len(codes)} communes ({updated} enrichies)")

                # Pause pour respecter le rate limiting INSEE (10 req/s)
                await asyncio.sleep(0.5)

        print(f"✅ Revenus INSEE chargés : {updated} communes enrichies")
        return updated

    return asyncio.run(run())


def log_insee(**context):
    """Log le résultat dans pipeline_log."""
    import asyncpg

    async def run():
        conn = await asyncpg.connect(IMMO_DB_URL)
        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, run_id, source, type, status, message)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, "insee_loader", str(context.get("run_id", "")),
             "INSEE Filosofi", "insee", "success",
             "Revenus médians chargés")
        await conn.close()

    asyncio.run(run())


with DAG(
    dag_id="insee_loader",
    description="Chargement revenus médians INSEE (Filosofi) par commune",
    schedule="0 4 1 1 *",   # 1er janvier à 4h (annuel)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["insee", "revenus", "ingestion"],
    doc_md="""
## INSEE Loader DAG

Charge les revenus médians par commune depuis la base Filosofi INSEE.

**Schedule :** annuel (1er janvier)

**Prérequis :** token INSEE gratuit sur https://api.insee.fr/

**⚠ Note :** données avec ~18 mois de décalage structurel.

**Table alimentée :** `communes.revenu_median_annuel`
    """,
) as dag:

    t1 = PythonOperator(
        task_id="check_token",
        python_callable=check_token,
    )

    t2 = PythonOperator(
        task_id="load_revenus_filosofi",
        python_callable=load_revenus_filosofi,
        execution_timeout=timedelta(hours=4),
    )

    t3 = PythonOperator(
        task_id="log_pipeline",
        python_callable=log_insee,
    )

    t1 >> t2 >> t3
