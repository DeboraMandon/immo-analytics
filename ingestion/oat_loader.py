"""
oat_loader.py — Chargement OAT 10 ans France

Source  : FRED (Federal Reserve Bank of St. Louis)
Série   : IRLTLT01FRM156N — Long-Term Government Bond Yields: 10-year France
Licence : Publique, gratuite avec clé API FRED (inscription gratuite)
Table   : oat_historique

L'OAT 10 ans est la référence principale pour les taux de crédit immobilier.
Une hausse de l'OAT entraine une hausse des taux bancaires dans les semaines suivantes.

Usage :
    python oat_loader.py
    python oat_loader.py --depuis 2015-01-01

Variables d'environnement :
    DATABASE_URL  : URL PostgreSQL
    FRED_API_KEY  : Clé API FRED (gratuite sur https://fredaccount.stlouisfed.org/apikeys)
"""
import argparse
import asyncio
import logging
import os
import time
from datetime import datetime

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://immo_user:immo_secret@postgres:5432/immo_dashboard",
)
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

FRED_URL = (
    "https://api.stlouisfed.org/fred/series/observations"
    "?series_id=IRLTLT01FRM156N"
    "&api_key={api_key}"
    "&file_type=json"
    "&observation_start={depuis}"
    "&frequency=m"
    "&aggregation_method=avg"
)


async def fetch_oat(depuis: str) -> list[dict]:
    if not FRED_API_KEY:
        raise ValueError(
            "FRED_API_KEY non configurée. "
            "Clé gratuite sur : https://fredaccount.stlouisfed.org/apikeys"
        )

    url = FRED_URL.format(api_key=FRED_API_KEY, depuis=depuis)
    logger.info(f"Téléchargement OAT FRED depuis {depuis}...")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    rows = [
        {"date_obs": obs["date"], "valeur": float(obs["value"])}
        for obs in data.get("observations", [])
        if obs["value"] != "."
    ]

    logger.info(f"{len(rows)} observations OAT récupérées")
    return rows


async def insert_oat(conn, rows: list[dict]) -> int:
    inserted = 0
    for row in rows:
        try:
            date_obs = datetime.strptime(row["date_obs"], "%Y-%m-%d").date()
            await conn.execute("""
                INSERT INTO oat_historique (date_obs, valeur, source)
                VALUES ($1, $2, 'FRED/OCDE')
                ON CONFLICT (date_obs) DO UPDATE SET
                    valeur     = EXCLUDED.valeur,
                    source     = EXCLUDED.source,
                    updated_at = NOW()
            """, date_obs, row["valeur"])
            inserted += 1
        except Exception as e:
            logger.debug(f"Insert OAT {row['date_obs']}: {e}")
    return inserted


async def main():
    parser = argparse.ArgumentParser(description="Chargement OAT 10 ans (FRED)")
    parser.add_argument("--depuis", default="2015-01-01")
    args = parser.parse_args()

    t0   = time.monotonic()
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        rows     = await fetch_oat(args.depuis)
        inserted = await insert_oat(conn, rows)

        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, source, type, status, records, duration_s)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, "oat_loader", "FRED/OCDE", "oat", "success",
             inserted, round(time.monotonic()-t0, 2))

        logger.info(f"✅ OAT chargé — {inserted} observations en {time.monotonic()-t0:.1f}s")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
