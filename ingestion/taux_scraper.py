"""
taux_scraper.py — Scraping taux de crédit immobilier

Sources : CAFPI (page publique baromètre mensuel)
Table   : taux_nationaux

⚠ Taux indicatifs, hors assurance, profil standard.
  Vérifier les CGU avant usage commercial.

Usage :
    python taux_scraper.py
"""
import asyncio
import logging
import os
import re
import time
from datetime import date

import asyncpg
import httpx
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://immo_user:immo_secret@postgres:5432/immo_dashboard",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ImmoDashboard/1.0; "
        "+https://github.com/DeboraMandon/immo-analytics)"
    ),
}

# Données de repli — à mettre à jour manuellement chaque mois
# Source : CAFPI baromètre public
FALLBACK = [
    {"duree_ans": 10, "taux_moyen": 2.99, "taux_min": 2.70, "taux_max": 3.20, "taux_usure": 5.19},
    {"duree_ans": 15, "taux_moyen": 3.20, "taux_min": 2.90, "taux_max": 3.45, "taux_usure": 5.19},
    {"duree_ans": 20, "taux_moyen": 3.33, "taux_min": 3.05, "taux_max": 3.55, "taux_usure": 5.19},
    {"duree_ans": 25, "taux_moyen": 3.43, "taux_min": 3.20, "taux_max": 3.65, "taux_usure": 5.19},
]


async def scrape_cafpi() -> list[dict]:
    """Parse le baromètre CAFPI."""
    try:
        async with httpx.AsyncClient(timeout=15, headers=HEADERS, follow_redirects=True) as client:
            resp = await client.get("https://www.cafpi.fr/credit-immobilier/barometre-taux")
            resp.raise_for_status()
    except Exception as e:
        logger.warning(f"CAFPI indisponible : {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    pct  = re.compile(r"(\d{1,2}[,\.]\d{2,3})\s*%")
    rows = []

    for table in soup.find_all("table"):
        if not any(f"{d} ans" in table.get_text() for d in [15, 20, 25]):
            continue
        for row in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            for duree in [10, 15, 20, 25]:
                if cells and str(duree) in cells[0]:
                    vals = [float(m.group(1).replace(",", "."))
                            for c in cells[1:] for m in [pct.search(c)] if m]
                    if vals:
                        rows.append({
                            "duree_ans":  duree,
                            "taux_moyen": vals[0],
                            "taux_min":   min(vals) if len(vals) > 1 else None,
                            "taux_max":   max(vals) if len(vals) > 1 else None,
                            "source":     "CAFPI",
                        })

    logger.info(f"CAFPI : {len(rows)} taux récupérés")
    return rows


async def insert_taux(conn, rows: list[dict], source: str) -> int:
    today    = date.today().replace(day=1)
    inserted = 0
    for r in rows:
        await conn.execute("""
            INSERT INTO taux_nationaux
                (date_obs, duree_ans, taux_moyen, taux_min, taux_max, taux_usure, source)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (date_obs, duree_ans, source) DO UPDATE SET
                taux_moyen = EXCLUDED.taux_moyen,
                taux_min   = EXCLUDED.taux_min,
                taux_max   = EXCLUDED.taux_max,
                taux_usure = EXCLUDED.taux_usure,
                scraped_at = NOW()
        """, today, r["duree_ans"], r["taux_moyen"],
             r.get("taux_min"), r.get("taux_max"), r.get("taux_usure"), source)
        inserted += 1
    return inserted


async def main():
    t0   = time.monotonic()
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        rows   = await scrape_cafpi()
        source = "CAFPI"

        if not rows:
            logger.warning("Scraping échoué — utilisation données de repli")
            rows   = FALLBACK
            source = "CAFPI_manual"

        n = await insert_taux(conn, rows, source)

        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, source, type, status, records, duration_s)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, "taux_scraper", source, "taux", "success", n, round(time.monotonic()-t0, 2))

        logger.info(f"✅ {n} taux insérés (source: {source}) en {time.monotonic()-t0:.1f}s")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
