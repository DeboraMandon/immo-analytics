"""
oat_loader.py — Chargement OAT 10 ans Banque de France

Source  : API publique Banque de France (webstat.banque-france.fr)
Série   : FM.M.FR.EUR.FR2.BB.U2_1S.YLD (OAT 10 ans France)
Table   : oat_historique

L'OAT 10 ans est la référence principale pour les taux de crédit immobilier.
Une hausse de l'OAT → hausse des taux bancaires dans les semaines suivantes.

Usage :
    python oat_loader.py
    python oat_loader.py --depuis 2020-01
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

# API Banque de France — données en accès libre, pas de token requis
BDF_API_URL = (
    "https://webstat.banque-france.fr/api/download/bdf/"
    "FM.M.FR.EUR.FR2.BB.U2_1S.YLD"
    "?format=json&startPeriod={depuis}&endPeriod={jusqua}"
)


async def fetch_oat(depuis: str, jusqua: str) -> list[dict]:
    """Récupère la série OAT 10 ans depuis l'API Banque de France."""
    url = BDF_API_URL.format(depuis=depuis, jusqua=jusqua)
    logger.info(f"Téléchargement OAT {depuis} → {jusqua}")

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Banque de France API error: {e}")
            return []

    # Parser la réponse JSON Banque de France
    # Structure : data → dataSets → [0] → series → {"0:0:0:0:0:0:0:0": {observations}}
    rows = []
    try:
        series = data["dataSets"][0]["series"]
        observations = list(series.values())[0]["observations"]
        periods = data["structure"]["dimensions"]["observation"][0]["values"]

        for i, period_info in enumerate(periods):
            period = period_info["id"]  # Format : "2024-01"
            obs    = observations.get(str(i))
            if obs and obs[0] is not None:
                rows.append({
                    "period": period,
                    "valeur": float(obs[0]),
                })
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Erreur parsing BdF: {e}")
        return []

    logger.info(f"{len(rows)} observations OAT récupérées")
    return rows


async def insert_oat(conn, rows: list[dict]) -> int:
    """Insère les données OAT en base."""
    inserted = 0
    for row in rows:
        try:
            # Convertir "2024-01" en date
            date_obs = datetime.strptime(row["period"] + "-01", "%Y-%m-%d").date()
            await conn.execute("""
                INSERT INTO oat_historique (date_obs, valeur, source)
                VALUES ($1, $2, 'Banque de France')
                ON CONFLICT (date_obs) DO UPDATE SET
                    valeur     = EXCLUDED.valeur,
                    updated_at = NOW()
            """, date_obs, row["valeur"])
            inserted += 1
        except Exception as e:
            logger.debug(f"Insert OAT {row['period']}: {e}")
    return inserted


async def main():
    parser = argparse.ArgumentParser(description="Chargement OAT 10 ans Banque de France")
    parser.add_argument("--depuis",  default="2015-01", help="Période de début (YYYY-MM)")
    parser.add_argument("--jusqua",  default=datetime.now().strftime("%Y-%m"),
                        help="Période de fin (YYYY-MM)")
    args = parser.parse_args()

    t0   = time.monotonic()
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        rows     = await fetch_oat(args.depuis, args.jusqua)
        inserted = await insert_oat(conn, rows)

        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, source, type, status, records, duration_s)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, "oat_loader", "Banque de France", "oat", "success",
             inserted, round(time.monotonic() - t0, 2))

        logger.info(f"✅ OAT chargé — {inserted} observations en {time.monotonic()-t0:.1f}s")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
