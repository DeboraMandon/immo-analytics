"""
insee_loader.py — Chargement revenus médians INSEE par commune

Source    : API INSEE Filosofi
Table     : communes (revenu_median_annuel, revenu_annee_ref)

Usage :
    python insee_loader.py

Variables d'environnement :
    DATABASE_URL      : URL de connexion PostgreSQL
    INSEE_API_TOKEN   : Token API INSEE (gratuit sur https://api.insee.fr/)

Note : données avec ~18 mois de décalage structurel (processus INSEE).
"""
import asyncio
import logging
import os
import time

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL    = os.environ.get("DATABASE_URL", "postgresql://immo_user:immo_secret@postgres:5432/immo_dashboard")
INSEE_API_TOKEN = os.environ.get("INSEE_API_TOKEN", "")

# Année de référence Filosofi disponible (décalage ~18 mois)
ANNEE_REF = 2022


async def load_revenus_filosofi(conn, token: str) -> int:
    """
    Charge les revenus médians par commune depuis l'API INSEE Filosofi.
    Traitement par batch de 50 communes pour respecter le rate limiting.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    # Récupérer tous les codes communes en base
    codes = await conn.fetch(
        "SELECT code_insee FROM communes WHERE latitude IS NOT NULL ORDER BY code_insee"
    )
    logger.info(f"{len(codes)} communes à enrichir (revenus {ANNEE_REF})")

    updated = 0
    batch_size = 50

    async with httpx.AsyncClient(headers=headers, timeout=15) as client:
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i + batch_size]

            for record in batch:
                code = record["code_insee"]
                url = (
                    f"https://api.insee.fr/donnees-locales/V0.1/donnees/"
                    f"geo-FILOSOFI_DISP_MED@FILOSOFI-{ANNEE_REF}/COM-{code}"
                )
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    valeur = (
                        data.get("Cellule", [{}])[0].get("Valeur")
                    )
                    if valeur:
                        await conn.execute("""
                            UPDATE communes
                            SET revenu_median_annuel = $1,
                                revenu_annee_ref     = $2,
                                updated_at           = NOW()
                            WHERE code_insee = $3
                        """, float(valeur), ANNEE_REF, code)
                        updated += 1
                except Exception as e:
                    logger.debug(f"Commune {code} : {e}")

            if i % 500 == 0 and i > 0:
                logger.info(f"  Progression : {i}/{len(codes)} ({updated} enrichies)")

            # Pause pour respecter le rate limiting INSEE (~10 req/s)
            await asyncio.sleep(0.3)

    return updated


async def main():
    if not INSEE_API_TOKEN:
        raise ValueError(
            "INSEE_API_TOKEN non configuré. "
            "Créer un compte gratuit sur https://api.insee.fr/ "
            "puis ajouter le token dans .env"
        )

    t0 = time.monotonic()
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        updated = await load_revenus_filosofi(conn, INSEE_API_TOKEN)
        duration = round(time.monotonic() - t0, 2)

        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, source, type, status, records, duration_s)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, "insee_loader", "INSEE Filosofi", "insee", "success", updated, duration)

        logger.info(f"✅ {updated} communes enrichies en {duration}s")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, source, type, status, message)
            VALUES ($1, $2, $3, $4, $5)
        """, "insee_loader", "INSEE", "insee", "error", str(e))
        raise

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
