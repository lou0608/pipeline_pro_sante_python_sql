# -*- coding: utf-8 -*-
"""
staging_loader.py
Chargement des données Ameli (Parquet -> fallback CSV) filtrées par année,
préparation du DataFrame, et UPSERT dans Postgres (staging).

- URL d'export ODS v2.1 correcte (dataset slug + refine + limit)
- Fallback CSV si Parquet indisponible
- Année résolue au RUNTIME (compatible pipeline_runner --year)
"""

import os
import logging
from io import StringIO
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from ops_logger import log_volume
from datetime import date


# ========= LOGGING =========
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ameli_loader")


# ========= PARAMS =========
# sauvegarde locale activée par défaut (brut + CSV préparé)
SAVE_LOCAL = os.getenv("SAVE_LOCAL", "true").lower() in {
    "1", "true", "yes", "y"}

# Dossier de sortie
DROP_DIR = Path(
    os.getenv("DROP_DIR", r"C:\Users\loudo\Downloads\Pipeline_Python\med_demo\drop")).expanduser()

# Colonnes attendues par la table de staging (ordre strict)
COLS = [
    "annee", "profession_sante", "region", "libelle_region",
    "departement", "libelle_departement",
    "classe_age", "libelle_classe_age", "libelle_sexe",
    "effectif", "densite",
    "vision_generale_all", "vision_generale_prescriptions", "vision_profession_territoire"
]

# ========= CONNEXION POSTGRES =========

PG_CONN = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE", "Health_Professional"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD"),  # pas de défaut
)
if not PG_CONN["password"]:
    raise RuntimeError("PGPASSWORD manquant (env/Secrets).")


# ========= ETAPE 1 : INGESTION =========
def fetch_parquet_df(year: Optional[int] = None) -> pd.DataFrame:
    """
    Télécharge l'export depuis data.ameli.fr :
    - essaie d'abord Parquet, puis fallback CSV
    - filtre sur l'année avec refine=annee:YYYY
    - limit=-1 pour tout rapatrier
    - teste deux slugs (nouveau + ancien) pour compatibilité
    """
    from urllib.parse import urlencode

    DATASET_IDS = [
        "demographie-effectifs-et-les-densites",  # slug courant de la page Export
        "professionnels-de-sante-liberaux-effectif-et-densite-par-tranche-dage-sexe-et-territoire",  # ancien
    ]

    # paramètres d’export
    params = [("limit", -1)]
    if year is not None:
        # Explore v2.1 : refine=field:value
        params.append(("refine", f"annee:{year}"))

    errors = []
    for ds in DATASET_IDS:
        base = f"https://data.ameli.fr/api/explore/v2.1/catalog/datasets/{ds}/exports"

        # 1) Parquet
        parquet_url = f"{base}/parquet?{urlencode(params, doseq=True)}"
        log.info("Lecture Parquet… %s", parquet_url)
        try:
            df = pd.read_parquet(parquet_url, engine="pyarrow")
            return df
        except Exception as e:
            errors.append(f"PARQUET {ds} -> {e}")
            log.warning("Parquet KO (%s). On tente le CSV…", e)

        # 2) CSV
        csv_url = f"{base}/csv?{urlencode(params, doseq=True)}"
        log.info("Lecture CSV… %s", csv_url)
        try:
            df = pd.read_csv(csv_url)
            return df
        except Exception as e:
            errors.append(f"CSV {ds} -> {e}")
            log.warning("CSV KO (%s).", e)

    raise RuntimeError("Aucun export accessible :\n" + "\n".join(errors))


# ========= ETAPE 2 : PREPARATION =========
def prepare_dataframe(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    - Aligne sur COLS (ajoute les colonnes manquantes)
    - Normalise 'annee' (date -> année Int64, sinon to_numeric)
    - Filtre par 'year'
    - Cast 'effectif'/'densite'
    """
    log.info("Préparation du DataFrame…")
    log.debug("Colonnes reçues: %s", list(df.columns))

    # 1) Colonnes manquantes
    missing = [c for c in COLS if c not in df.columns]
    if missing:
        log.warning("Colonnes manquantes → ajout en None: %s", missing)
        for c in missing:
            df[c] = None
    df = df[COLS]

    # 2) Normaliser 'annee'
    try:
        a_dt = pd.to_datetime(df["annee"], errors="coerce", utc=False)
        if a_dt.notna().any():
            df["annee"] = a_dt.dt.year.astype("Int64")
            log.debug("'annee' détectée comme date → extraction année OK")
        else:
            df["annee"] = pd.to_numeric(
                df["annee"], errors="coerce").astype("Int64")
            log.debug("'annee' non date → conversion numérique OK")
    except Exception as e:
        log.warning(
            "to_datetime('annee') a échoué (%s) → fallback to_numeric", e)
        df["annee"] = pd.to_numeric(
            df["annee"], errors="coerce").astype("Int64")

    # 3) Filtre année (filet de sécurité)
    before = len(df)
    df = df[df["annee"] == year].copy()
    log.info("Filtre année %s : %s → %s lignes",
             year, f"{before:,}", f"{len(df):,}")

    # 4) Casts numériques
    df["effectif"] = pd.to_numeric(df["effectif"], errors="coerce")
    df["densite"] = pd.to_numeric(df["densite"], errors="coerce")

    log.info("Préparation terminée : %s lignes", f"{len(df):,}")
    log.debug("dtypes finaux: %s", {k: str(v) for k, v in df.dtypes.items()})
    return df


# ========= ETAPE 3 : UPSERT STAGING =========
def upsert_to_staging(df: pd.DataFrame, year: int) -> None:
    """
    COPY vers table temporaire -> UPSERT vers pro_sante.stg_pro_sante_raw
    Contrainte/Index unique requis sur :
    (annee, region, departement, profession_sante, classe_age, libelle_sexe)
    """
    if df.empty:
        log.warning("Aucune ligne pour %s → rien à insérer", year)
        return

    # Buffer TSV
    buf = StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t", na_rep="")
    buf.seek(0)

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            log.info("Création table temporaire tmp_stg…")
            cur.execute(
                """
                CREATE TEMP TABLE tmp_stg
                (LIKE "pro_sante".stg_pro_sante_raw INCLUDING DEFAULTS)
                ON COMMIT DROP;
                """
            )

            log.info("COPY vers tmp_stg (%s lignes)…", f"{len(df):,}")
            cur.copy_from(buf, "tmp_stg", sep="\t", null="", columns=COLS)

            log.info("UPSERT vers stg_pro_sante_raw…")
            cur.execute(
                """
                INSERT INTO "pro_sante".stg_pro_sante_raw (
                  annee, profession_sante, region, libelle_region,
                  departement, libelle_departement,
                  classe_age, libelle_classe_age, libelle_sexe,
                  effectif, densite,
                  vision_generale_all, vision_generale_prescriptions, vision_profession_territoire
                )
                SELECT
                  annee, profession_sante, region, libelle_region,
                  departement, libelle_departement,
                  classe_age, libelle_classe_age, libelle_sexe,
                  effectif, densite,
                  vision_generale_all, vision_generale_prescriptions, vision_profession_territoire
                FROM tmp_stg
                WHERE annee = %s
                ON CONFLICT (annee, region, departement, profession_sante, classe_age, libelle_sexe)
                DO UPDATE SET
                  effectif = EXCLUDED.effectif,
                  densite  = EXCLUDED.densite;
                """,
                (year,),
            )
        conn.commit()
        log.info("[OK] UPSERT terminé pour %s", year)


# ========= ENTRYPOINT =========
def main(year: Optional[int] = None):
    """
    Pipeline staging :
      1) téléchargement export filtré sur l'année
      2) préparation DataFrame
      3) UPSERT vers staging
      + sauvegardes locales optionnelles
    """
    # Année résolue au runtime (ENV > argument > défaut)
    y = int(os.getenv("YEAR", year if year is not None else 2022))

    log.info("=== DÉBUT STAGING ===")
    log.info("Paramètres: YEAR=%s | SAVE_LOCAL=%s", y, SAVE_LOCAL)
    log.info("DROP_DIR: %s", DROP_DIR)

    # 1) Ingestion
    df_raw = fetch_parquet_df(year=y)

    # (optionnel) sauvegarde du brut
    if SAVE_LOCAL:
        try:
            DROP_DIR.mkdir(parents=True, exist_ok=True)
            raw_parquet = DROP_DIR / f"pro_sante_raw_{y}.parquet"
            df_raw.to_parquet(raw_parquet, index=False)
            log.info("Parquet brut sauvegardé → %s", raw_parquet)
        except Exception as e:
            log.warning("Impossible de sauvegarder le Parquet brut: %s", e)

    # 2) Préparation
    df_ready = prepare_dataframe(df_raw, y)

    # (optionnel) export CSV d’inspection
    if SAVE_LOCAL:
        try:
            out_csv = DROP_DIR / f"pro_sante_{y}_ready.csv"
            df_ready.to_csv(out_csv, index=False, sep=";")
            size_mb = out_csv.stat().st_size / (1024 * 1024)
            log.info("CSV préparé écrit → %s (%.1f Mo)", out_csv, size_mb)
        except Exception as e:
            log.warning("Impossible d’écrire le CSV préparé: %s", e)

    # 3) UPSERT staging
    upsert_to_staging(df_ready, y)
    # 4) Compte des lignes staging pour l'année et log volume annuel
    with psycopg2.connect(**PG_CONN) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) 
            FROM "pro_sante".stg_pro_sante_raw
            WHERE annee = %s
              AND region <> '99'
              AND departement <> '999'
              AND libelle_sexe IN ('hommes','femmes')
              AND classe_age <> 'tout_age'
        """, (y,))
        (cnt,) = cur.fetchone()

    log_volume(
        pipeline="pro_sante",
        entity="stg_pro_sante_raw",
        as_of_date=date(y, 12, 31),
        volume=cnt,
        expected_min=1000,   # seuil initial simple
        agg_window='Y',
        extra={"year": y}
    )

    log.info("=== FIN STAGING ===")


if __name__ == "__main__":
    # Permet aussi l'exécution directe du fichier :
    #   python staging_loader.py  (utilise YEAR de l'ENV ou 2022)
    #   YEAR=2021 python staging_loader.py
    main()
