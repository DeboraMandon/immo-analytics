"""
predict_taux.py — Prédiction des taux de crédit immobilier (6 mois)

Modèle : Prophet (Meta) — série temporelle avec saisonnalité
Features utilisées :
  - Historique taux mensuels (taux_nationaux)
  - OAT 10 ans comme régresseur externe (oat_historique)

Table cible : predictions_taux

Usage :
    python predict_taux.py
    python predict_taux.py --duree 20 --horizon 6

⚠ Fiabilité limitée à 6 mois maximum.
  Les taux dépendent de facteurs macro-économiques imprévisibles.
  Ces prédictions sont des estimations statistiques, pas des certitudes.
"""
import argparse
import asyncio
import logging
import os
import time
from datetime import datetime

import asyncpg
import pandas as pd
from prophet import Prophet

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://immo_user:immo_secret@postgres:5432/immo_dashboard",
)


async def load_data(conn, duree_ans: int) -> pd.DataFrame:
    """Charge l'historique des taux depuis la base."""
    rows = await conn.fetch("""
        SELECT
            DATE_TRUNC('month', date_obs)::date AS ds,
            AVG(taux_moyen)                     AS y,
            AVG(t.valeur)                       AS oat
        FROM taux_nationaux tn
        LEFT JOIN oat_historique t
            ON DATE_TRUNC('month', t.date_obs) = DATE_TRUNC('month', tn.date_obs)
        WHERE tn.duree_ans = $1
        GROUP BY DATE_TRUNC('month', date_obs)
        ORDER BY ds
    """, duree_ans)

    df = pd.DataFrame([dict(r) for r in rows])
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"]  = df["y"].astype(float)
    return df


def train_predict(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Entraîne Prophet et génère les prédictions."""
    m = Prophet(
        seasonality_mode="additive",
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,  # Conservateur — marché immobilier peu volatile
    )

    # Ajouter l'OAT comme régresseur externe si disponible
    if "oat" in df.columns and df["oat"].notna().sum() > 5:
        df["oat"] = df["oat"].fillna(method="ffill")
        m.add_regressor("oat")
        logger.info("OAT 10 ans ajouté comme régresseur")

    m.fit(df[["ds", "y"] + (["oat"] if "oat" in df.columns and df["oat"].notna().sum() > 5 else [])])

    # Créer le dataframe futur
    future = m.make_future_dataframe(periods=horizon, freq="MS")

    if "oat" in df.columns and df["oat"].notna().sum() > 5:
        # Extrapoler l'OAT avec la dernière valeur connue (conservative)
        last_oat = df["oat"].dropna().iloc[-1]
        future["oat"] = future["ds"].map(
            lambda d: df.set_index("ds")["oat"].get(d, last_oat)
        ).fillna(last_oat)

    forecast = m.predict(future)

    # Garder uniquement les prédictions futures
    last_date = df["ds"].max()
    return forecast[forecast["ds"] > last_date][["ds", "yhat", "yhat_lower", "yhat_upper"]]


async def save_predictions(conn, predictions: pd.DataFrame, duree_ans: int) -> int:
    """Sauvegarde les prédictions en base."""
    run_date = datetime.now().date()
    inserted = 0

    for _, row in predictions.iterrows():
        await conn.execute("""
            INSERT INTO predictions_taux
                (date_prediction, duree_ans, taux_predit, taux_lower, taux_upper,
                 modele, run_date)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (date_prediction, duree_ans, modele) DO UPDATE SET
                taux_predit = EXCLUDED.taux_predit,
                taux_lower  = EXCLUDED.taux_lower,
                taux_upper  = EXCLUDED.taux_upper,
                run_date    = EXCLUDED.run_date
        """,
            row["ds"].date(), duree_ans,
            round(float(row["yhat"]), 3),
            round(float(row["yhat_lower"]), 3),
            round(float(row["yhat_upper"]), 3),
            "Prophet", run_date,
        )
        inserted += 1

    return inserted


async def main():
    parser = argparse.ArgumentParser(description="Prédiction taux crédit immobilier")
    parser.add_argument("--duree",   type=int, default=20,
                        choices=[10, 15, 20, 25], help="Durée du prêt en années")
    parser.add_argument("--horizon", type=int, default=6,
                        help="Horizon de prédiction en mois (max 6)")
    parser.add_argument("--all-durees", action="store_true",
                        help="Prédire pour toutes les durées (10, 15, 20, 25 ans)")
    args = parser.parse_args()

    if args.horizon > 6:
        logger.warning("Horizon > 6 mois — fiabilité très limitée. Réduction à 6 mois.")
        args.horizon = 6

    durees = [10, 15, 20, 25] if args.all_durees else [args.duree]

    t0   = time.monotonic()
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        total = 0
        for duree in durees:
            logger.info(f"Prédiction taux {duree} ans — horizon {args.horizon} mois")

            df = await load_data(conn, duree)
            if len(df) < 12:
                logger.warning(f"Données insuffisantes pour {duree} ans ({len(df)} mois) — minimum 12 requis")
                continue

            predictions = train_predict(df, args.horizon)
            n           = await save_predictions(conn, predictions, duree)
            total      += n
            logger.info(f"  → {n} prédictions sauvegardées pour {duree} ans")

        await conn.execute("""
            INSERT INTO pipeline_log (dag_id, source, type, status, records, duration_s)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, "predict_taux", "Prophet", "prediction", "success",
             total, round(time.monotonic()-t0, 2))

        logger.info(f"✅ Prédictions terminées — {total} entrées en {time.monotonic()-t0:.1f}s")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
