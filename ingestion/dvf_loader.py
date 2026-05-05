"""
dvf_loader.py — Chargement DVF (Demandes de Valeurs Foncières)

Source  : https://files.data.gouv.fr/geo-dvf/latest/
Licence : Licence Ouverte Etalab
Tables  : communes, prix_immobilier

Usage :
    python dvf_loader.py --all-france --annee 2025
    python dvf_loader.py --departements 75,69,13 --annee 2025 --skip-geo

Départements exclus DVF : 57 (Moselle), 67 (Bas-Rhin), 68 (Haut-Rhin), 976 (Mayotte)
→ Ces territoires utilisent le livre foncier local, non intégré à DVF.
"""
import argparse
import asyncio
import csv
import io
import json
import logging
import os
import time
from statistics import median
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://immo_user:immo_secret@postgres:5432/immo_dashboard",
)

DVF_COMMUNE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/communes/{dept}/{code}.csv"
DVF_DEPT_INDEX  = "https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/communes/{dept}/"
GEO_API_URL     = "https://geo.api.gouv.fr/communes?codeDepartement={dept}&fields=code,nom,population,centre&format=json"

DEPT_EXCLUS = {"57", "67", "68", "976"}

ALL_DEPARTEMENTS = [
    "01","02","03","04","05","06","07","08","09","10",
    "11","12","13","14","15","16","17","18","19","21",
    "22","23","24","25","26","27","28","29","2A","2B",
    "30","31","32","33","34","35","36","37","38","39",
    "40","41","42","43","44","45","46","47","48","49",
    "50","51","52","53","54","55","56","58","59","60",
    "61","62","63","64","65","66","69","70","71","72",
    "73","74","75","76","77","78","79","80","81","82",
    "83","84","85","86","87","88","89","90","91","92",
    "93","94","95","971","972","973","974",
]

HEADERS = {"User-Agent": "ImmoDashboard/1.0 (+https://github.com/DeboraMandon/immo-analytics)"}


def fetch(url: str, timeout: int = 30) -> bytes | None:
    try:
        with urlopen(Request(url, headers=HEADERS), timeout=timeout) as r:
            return r.read()
    except HTTPError as e:
        if e.code == 404:
            return None
        logger.warning(f"HTTP {e.code} — {url}")
        return None
    except URLError as e:
        logger.error(f"URL error {url}: {e}")
        return None


def get_commune_codes(dept: str, annee: int) -> list[str]:
    url  = DVF_DEPT_INDEX.format(annee=annee, dept=dept.zfill(2))
    data = fetch(url, timeout=15)
    if not data:
        return []
    html  = data.decode("utf-8", errors="replace")
    codes = []
    for part in html.split(".csv"):
        idx = part.rfind(">")
        if idx >= 0:
            c = part[idx+1:].strip()
            if len(c) == 5 and (c.isdigit() or c.startswith("2")):
                codes.append(c)
    return codes


def parse_dvf_csv(content: str) -> dict:
    reader = csv.DictReader(io.StringIO(content))
    result = {
        "code_commune": None, "nom_commune": None, "code_departement": None,
        "appartement": [], "maison": [],
    }
    for row in reader:
        if result["code_commune"] is None:
            result["code_commune"]     = row.get("code_commune", "").strip()
            result["nom_commune"]      = row.get("nom_commune", "").strip()
            result["code_departement"] = row.get("code_departement", "").strip()

        if row.get("nature_mutation", "").strip() != "Vente":
            continue
        type_local = row.get("type_local", "").strip()
        if type_local not in ("Appartement", "Maison"):
            continue

        try:
            valeur  = float(row.get("valeur_fonciere",  "").replace(",", "."))
            surface = float(row.get("surface_reelle_bati", "").replace(",", "."))
        except (ValueError, TypeError):
            continue

        if valeur <= 0 or surface < 9 or surface > 1000:
            continue
        prix_m2 = valeur / surface
        if prix_m2 < 300 or prix_m2 > 50000:
            continue

        key = "appartement" if type_local == "Appartement" else "maison"
        result[key].append(prix_m2)

    return result


async def insert_commune(conn, parsed: dict, annee: int) -> int:
    code = parsed["code_commune"]
    if not code:
        return 0

    await conn.execute("""
        INSERT INTO communes (code_insee, nom, departement)
        VALUES ($1, $2, $3)
        ON CONFLICT (code_insee) DO NOTHING
    """, code, parsed["nom_commune"] or code, parsed["code_departement"] or "")

    inserted = 0
    for type_bien, key in [("appartement", "appartement"), ("maison", "maison")]:
        prix_list = parsed[key]
        if len(prix_list) < 3:
            continue
        await conn.execute("""
            INSERT INTO prix_immobilier
                (code_insee, annee, trimestre, type_bien,
                 prix_m2_moyen, prix_m2_median, nb_transactions, source)
            VALUES ($1,$2,NULL,$3,$4,$5,$6,'DVF')
            ON CONFLICT (code_insee, annee, trimestre, type_bien) DO UPDATE SET
                prix_m2_median  = EXCLUDED.prix_m2_median,
                prix_m2_moyen   = EXCLUDED.prix_m2_moyen,
                nb_transactions = EXCLUDED.nb_transactions,
                loaded_at       = NOW()
        """, code, annee, type_bien,
             round(sum(prix_list)/len(prix_list), 2),
             round(median(prix_list), 2),
             len(prix_list))
        inserted += 1
    return inserted


async def enrich_geo(conn, departements: list[str]) -> int:
    logger.info("Enrichissement géographique via geo.api.gouv.fr...")
    enriched = 0
    for dept in departements:
        data = fetch(GEO_API_URL.format(dept=dept), timeout=20)
        if not data:
            continue
        try:
            communes = json.loads(data)
        except json.JSONDecodeError:
            continue
        for c in communes:
            coords = c.get("centre", {}).get("coordinates", [None, None])
            lng, lat = coords[0], coords[1]
            try:
                await conn.execute("""
                    INSERT INTO communes
                        (code_insee, nom, departement, population, latitude, longitude, geom)
                    VALUES ($1,$2,$3,$4,$5::numeric,$6::numeric,
                        ST_SetSRID(ST_MakePoint($6::float8,$5::float8),4326))
                    ON CONFLICT (code_insee) DO UPDATE SET
                        nom        = EXCLUDED.nom,
                        population = COALESCE(EXCLUDED.population, communes.population),
                        latitude   = COALESCE(EXCLUDED.latitude,   communes.latitude),
                        longitude  = COALESCE(EXCLUDED.longitude,  communes.longitude),
                        geom       = COALESCE(EXCLUDED.geom,       communes.geom),
                        updated_at = NOW()
                """, c["code"], c["nom"], dept, c.get("population"), lat, lng)
                enriched += 1
            except Exception as e:
                logger.error(f"Geo insert {c['code']}: {e}")
    logger.info(f"Enrichissement terminé — {enriched} communes")
    return enriched


async def load_dvf(conn, departements: list[str], annee: int) -> int:
    total = 0
    t0    = time.monotonic()

    for i, dept in enumerate(departements):
        dept_norm = dept.zfill(2) if dept not in ("2A", "2B") else dept
        if dept in DEPT_EXCLUS:
            logger.warning(f"[{i+1}/{len(departements)}] Dept {dept} exclu (livre foncier local)")
            continue

        codes = get_commune_codes(dept_norm, annee)
        if not codes:
            logger.warning(f"[{i+1}/{len(departements)}] Dept {dept} — aucune commune trouvée")
            continue

        logger.info(f"[{i+1}/{len(departements)}] Dept {dept} — {len(codes)} communes")
        dept_total = 0

        for code in codes:
            url     = DVF_COMMUNE_URL.format(annee=annee, dept=dept_norm, code=code)
            content = fetch(url, timeout=30)
            if not content:
                continue
            try:
                parsed = parse_dvf_csv(content.decode("utf-8", errors="replace"))
                n      = await insert_commune(conn, parsed, annee)
                dept_total += n
            except Exception as e:
                logger.debug(f"Commune {code}: {e}")

        total += dept_total
        elapsed = time.monotonic() - t0
        logger.info(f"  ✓ {dept_total} entrées — total {total} en {elapsed:.0f}s")
        await asyncio.sleep(0.3)

    return total


async def main():
    parser = argparse.ArgumentParser(description="Chargement DVF")
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--all-france",   action="store_true")
    group.add_argument("--departements", default="75,69,13")
    parser.add_argument("--annee",    type=int, default=2025)
    parser.add_argument("--skip-geo", action="store_true", help="Ne pas recharger les coordonnées GPS")
    args = parser.parse_args()

    depts = ALL_DEPARTEMENTS if args.all_france else [d.strip() for d in args.departements.split(",")]
    logger.info(f"DVF {args.annee} — {len(depts)} départements")

    conn = await asyncpg.connect(DATABASE_URL)
    t0   = time.monotonic()

    try:
        if not args.skip_geo:
            await enrich_geo(conn, depts)

        total = await load_dvf(conn, depts, args.annee)

        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, source, type, status, records, duration_s)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, "dvf_loader", f"DVF {args.annee}", "dvf", "success",
             total, round(time.monotonic() - t0, 2))

        logger.info(f"✅ DVF {args.annee} terminé — {total} entrées en {time.monotonic()-t0:.0f}s")

    except Exception as e:
        logger.error(f"Erreur fatale : {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
