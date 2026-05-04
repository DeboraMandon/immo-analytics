"""
DVF Loader — Chargement des données Demandes de Valeurs Foncières
Source : https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/communes/{dept}/{code_commune}.csv

Format réel des fichiers DVF (vérifié sur data.gouv.fr) :
  Colonnes clés : code_commune, nom_commune, code_departement,
                  type_local (Appartement/Maison),
                  valeur_fonciere, surface_reelle_bati,
                  nature_mutation (on garde uniquement "Vente")

Stratégie : on lit les CSV par commune (un fichier par commune),
on calcule le prix médian au m² par type de bien, on insère en base.

Départements exclus DVF : 57 (Moselle), 67 (Bas-Rhin), 68 (Haut-Rhin), 976 (Mayotte)

Usage :
    python dvf_loader.py --all-france --annee 2024
    python dvf_loader.py --departements 75,69,13 --annee 2024
"""
import argparse
import asyncio
import csv
import io
import json
import logging
import os
import time
from collections import defaultdict
from statistics import median
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# ── Connexion DB ─────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://immo_user:immo_secret@postgres:5432/immo_dashboard",
)

def get_pg_url(url: str) -> str:
    """Normalise l'URL pour asyncpg (supprime le préfixe +asyncpg si présent)."""
    return url.replace("postgresql+asyncpg://", "postgresql://")

# ── URLs DVF ─────────────────────────────────────────────────────────────────
# Index des communes par département
DVF_DEPT_INDEX = "https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/communes/{dept}/"
# Fichier CSV par commune
DVF_COMMUNE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/communes/{dept}/{code}.csv"
# API data.gouv.fr pour détecter la dernière version disponible
DATAGOUV_DVF_API = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres-geolocalisees/"

# ── Constantes ────────────────────────────────────────────────────────────────
DEPT_EXCLUS = {"57", "67", "68", "976"}

# Tous les départements France métropolitaine + DOM
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

HEADERS = {
    "User-Agent": "ImmoDashboard/1.0 (open source; github.com/DeboraMandon/immo-dashboard)"
}


def fetch_url(url: str, timeout: int = 30) -> bytes | None:
    """Télécharge une URL avec gestion d'erreur propre."""
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except HTTPError as e:
        if e.code == 404:
            return None  # Commune sans données = normal
        logger.warning(f"HTTP {e.code} — {url}")
        return None
    except URLError as e:
        logger.error(f"URL error — {url} : {e}")
        return None


def get_commune_codes_for_dept(dept: str, annee: int) -> list[str]:
    """
    Récupère la liste des codes communes disponibles pour un département.
    L'index DVF liste les fichiers CSV disponibles sous forme HTML.
    """
    url = DVF_DEPT_INDEX.format(annee=annee, dept=dept.zfill(2))
    data = fetch_url(url, timeout=15)
    if not data:
        return []

    # Parse l'index HTML pour extraire les noms de fichiers .csv
    html = data.decode("utf-8", errors="replace")
    codes = []
    for part in html.split(".csv"):
        # Cherche le code commune juste avant ".csv"
        idx = part.rfind(">")
        if idx >= 0:
            candidate = part[idx+1:].strip()
            # Code commune = 5 chiffres (ou 2A/2B + 3 chiffres)
            if len(candidate) == 5 and (candidate.isdigit() or candidate.startswith("2")):
                codes.append(candidate)
    return codes


def parse_dvf_csv(content: str, annee: int) -> dict[str, dict]:
    """
    Parse un CSV DVF et retourne les prix médians par type de bien.

    Colonnes utilisées :
        - nature_mutation : on garde uniquement "Vente"
        - type_local : "Appartement" ou "Maison"
        - valeur_fonciere : prix de vente (float, séparateur = virgule dans certains fichiers)
        - surface_reelle_bati : surface en m²
        - code_commune, nom_commune, code_departement

    Retourne :
        {
          "code_commune": "75056",
          "nom_commune": "Paris",
          "code_departement": "75",
          "appartement": {"prix_m2_list": [8000, 9500, ...], "nb": 42},
          "maison": {"prix_m2_list": [...], "nb": 5},
        }
    """
    reader = csv.DictReader(io.StringIO(content))
    result = {
        "code_commune": None,
        "nom_commune": None,
        "code_departement": None,
        "appartement": {"prix_m2_list": [], "nb": 0},
        "maison":      {"prix_m2_list": [], "nb": 0},
    }

    for row in reader:
        # Métadonnées commune (on prend la première ligne valide)
        if result["code_commune"] is None:
            result["code_commune"]    = row.get("code_commune", "").strip()
            result["nom_commune"]     = row.get("nom_commune", "").strip()
            result["code_departement"]= row.get("code_departement", "").strip()

        # Filtres : uniquement les ventes
        if row.get("nature_mutation", "").strip() != "Vente":
            continue

        type_local = row.get("type_local", "").strip()
        if type_local not in ("Appartement", "Maison"):
            continue

        # Prix et surface
        valeur_raw  = row.get("valeur_fonciere", "").strip().replace(",", ".")
        surface_raw = row.get("surface_reelle_bati", "").strip().replace(",", ".")

        try:
            valeur  = float(valeur_raw)
            surface = float(surface_raw)
        except (ValueError, TypeError):
            continue

        # Filtres de cohérence
        if valeur <= 0 or surface <= 0:
            continue
        if surface < 9 or surface > 1000:   # < 9m² ou > 1000m² = aberrant
            continue

        prix_m2 = valeur / surface

        # Filtre prix aberrants
        if prix_m2 < 300 or prix_m2 > 50000:
            continue

        key = "appartement" if type_local == "Appartement" else "maison"
        result[key]["prix_m2_list"].append(prix_m2)
        result[key]["nb"] += 1

    return result


async def insert_commune_data(conn, parsed: dict, annee: int):
    """Insère ou met à jour les données d'une commune en base."""
    code = parsed["code_commune"]
    if not code:
        return 0

    # Upsert commune (nom + département)
    await conn.execute("""
        INSERT INTO communes (code_insee, nom, departement)
        VALUES ($1, $2, $3)
        ON CONFLICT (code_insee) DO UPDATE SET
            nom        = EXCLUDED.nom,
            updated_at = NOW()
        WHERE communes.nom = communes.code_insee  -- met à jour seulement si nom = code (placeholder)
    """, code, parsed["nom_commune"] or code, parsed["code_departement"] or "")

    inserted = 0
    for type_bien, key in [("appartement", "appartement"), ("maison", "maison")]:
        prix_list = parsed[key]["prix_m2_list"]
        nb        = parsed[key]["nb"]

        if nb < 3:  # Moins de 3 transactions = pas significatif statistiquement
            continue

        prix_median_val = round(median(prix_list), 2)
        prix_moyen_val  = round(sum(prix_list) / len(prix_list), 2)

        await conn.execute("""
            INSERT INTO prix_immobilier
                (code_insee, annee, trimestre, type_bien,
                 prix_m2_moyen, prix_m2_median, nb_transactions, source)
            VALUES ($1, $2, NULL, $3, $4, $5, $6, 'DVF')
            ON CONFLICT (code_insee, annee, trimestre, type_bien) DO UPDATE SET
                prix_m2_median  = EXCLUDED.prix_m2_median,
                prix_m2_moyen   = EXCLUDED.prix_m2_moyen,
                nb_transactions = EXCLUDED.nb_transactions,
                source          = 'DVF',
                loaded_at       = NOW()
        """, code, annee, type_bien, prix_moyen_val, prix_median_val, nb)
        inserted += 1

    return inserted


async def enrich_communes_geo(conn, departements: list[str]):
    """
    Enrichit les communes avec coordonnées GPS et population
    depuis l'API officielle geo.api.gouv.fr (gratuite, sans token).
    """
    logger.info("Enrichissement géographique via geo.api.gouv.fr...")
    enriched = 0

    for dept in departements:
        url = (
            f"https://geo.api.gouv.fr/communes"
            f"?codeDepartement={dept}"
            f"&fields=code,nom,population,centre,region"
            f"&format=json"
        )
        data = fetch_url(url, timeout=20)
        if not data:
            continue

        try:
            communes = json.loads(data)
        except json.JSONDecodeError:
            continue

        for c in communes:
            coords = c.get("centre", {}).get("coordinates", [None, None])
            lng, lat = coords[0], coords[1]
            pop = c.get("population")

            try:
                await conn.execute("""
                    INSERT INTO communes
                        (code_insee, nom, departement, population,
                         latitude, longitude, geom)
                    VALUES ($1, $2, $3, $4, $5, $6,
                        ST_SetSRID(ST_MakePoint($6::float8, $5::float8), 4326))
                    ON CONFLICT (code_insee) DO UPDATE SET
                        nom        = EXCLUDED.nom,
                        population = COALESCE(EXCLUDED.population, communes.population),
                        latitude   = COALESCE(EXCLUDED.latitude,   communes.latitude),
                        longitude  = COALESCE(EXCLUDED.longitude,  communes.longitude),
                        geom       = COALESCE(EXCLUDED.geom,       communes.geom),
                        updated_at = NOW()
                """, c["code"], c["nom"], dept, pop, lat, lng)
                enriched += 1
            except Exception as e:
                logger.debug(f"Geo upsert {c['code']}: {e}")

    logger.info(f"Enrichissement géo terminé — {enriched} communes")


async def check_dvf_update_available(annee_actuelle: int) -> bool:
    """
    Interroge l'API data.gouv.fr pour détecter si une nouvelle version DVF
    est disponible (mise à jour avril/octobre chaque année).
    Retourne True si une version plus récente que annee_actuelle est dispo.
    """
    data = fetch_url(DATAGOUV_DVF_API, timeout=10)
    if not data:
        return False
    try:
        meta = json.loads(data)
        last_modified = meta.get("last_modified", "")
        # DVF 2024 = disponible depuis avril 2025
        # DVF 2025 = disponible depuis avril 2026
        annee_dispo = int(last_modified[:4]) - 1 if last_modified else annee_actuelle
        if annee_dispo > annee_actuelle:
            logger.info(f"Nouvelle version DVF disponible : {annee_dispo}")
            return True
    except Exception:
        pass
    return False


async def load_dvf_for_departements(
    conn,
    departements: list[str],
    annee: int,
    pause_between_depts: float = 0.5,
) -> int:
    """
    Charge les données DVF pour une liste de départements.
    Télécharge chaque fichier commune par commune.
    """
    total_communes = 0
    total_prix     = 0
    t0 = time.monotonic()

    for i, dept in enumerate(departements):
        dept_norm = dept.zfill(2) if dept not in ("2A", "2B") else dept

        if dept in DEPT_EXCLUS:
            logger.warning(f"[{i+1}/{len(departements)}] Dept {dept} exclu (Alsace-Moselle/Mayotte)")
            continue

        logger.info(f"[{i+1}/{len(departements)}] Département {dept}...")

        # Récupérer la liste des communes du département
        commune_codes = get_commune_codes_for_dept(dept_norm, annee)

        if not commune_codes:
            logger.warning(f"  → Aucune commune trouvée pour dept {dept}")
            continue

        logger.info(f"  → {len(commune_codes)} communes à traiter")

        dept_communes = 0
        dept_prix     = 0

        for code in commune_codes:
            url = DVF_COMMUNE_URL.format(
                annee=annee, dept=dept_norm, code=code
            )
            content = fetch_url(url, timeout=30)
            if not content:
                continue

            try:
                text   = content.decode("utf-8", errors="replace")
                parsed = parse_dvf_csv(text, annee)
                n      = await insert_commune_data(conn, parsed, annee)
                if n > 0:
                    dept_communes += 1
                    dept_prix     += n
            except Exception as e:
                logger.debug(f"  Erreur commune {code}: {e}")

        total_communes += dept_communes
        total_prix     += dept_prix

        elapsed = time.monotonic() - t0
        logger.info(
            f"  ✓ {dept_communes} communes, {dept_prix} entrées prix "
            f"— Total : {total_communes} communes en {elapsed:.0f}s"
        )

        # Pause courtoise entre départements
        await asyncio.sleep(pause_between_depts)

    return total_prix


async def main():
    parser = argparse.ArgumentParser(
        description="Chargement DVF — Demandes de Valeurs Foncières"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--all-france",
        action="store_true",
        help="Charger tous les départements France (long ~30-60 min)"
    )
    group.add_argument(
        "--departements",
        default="75,69,13,31,06,44,33,59,35,67",
        help="Codes départements séparés par virgule (ex: 75,69,13)"
    )
    parser.add_argument("--annee",  type=int, default=2024, help="Année DVF")
    parser.add_argument("--skip-geo", action="store_true",
                        help="Ne pas enrichir les coordonnées GPS")
    parser.add_argument("--check-update", action="store_true",
                        help="Vérifie si une nouvelle version DVF est disponible")
    args = parser.parse_args()

    # Vérification de mise à jour uniquement
    if args.check_update:
        available = await check_dvf_update_available(args.annee)
        print(f"Nouvelle version DVF disponible : {available}")
        return

    # Liste des départements
    if args.all_france:
        departements = ALL_DEPARTEMENTS
        logger.info(f"Mode France entière — {len(departements)} départements")
    else:
        departements = [d.strip() for d in args.departements.split(",")]

    logger.info(f"DVF {args.annee} — {len(departements)} départements à charger")
    logger.info(f"Connexion : {DATABASE_URL.split('@')[1]}")  # masque le mdp

    # Connexion DB
    try:
        conn = await asyncpg.connect(get_pg_url(DATABASE_URL))
    except Exception as e:
        logger.error(f"Impossible de se connecter à la base : {e}")
        return

    t_start = time.monotonic()

    try:
        # 1 — Enrichissement géographique (coordonnées + population)
        if not args.skip_geo:
            await enrich_communes_geo(conn, departements)

        # 2 — Chargement DVF
        total = await load_dvf_for_departements(conn, departements, args.annee)

        elapsed = time.monotonic() - t_start
        logger.info(
            f"\n✅ Chargement terminé en {elapsed:.0f}s\n"
            f"   {total} entrées de prix insérées\n"
            f"   Source : DVF DGFiP {args.annee} — data.gouv.fr"
        )

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
