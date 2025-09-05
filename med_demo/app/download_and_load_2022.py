import os
import sys
from io import StringIO
from pathlib import Path
import urllib.parse as ul
import pandas as pd
import psycopg2

# ========= PARAMS =========
YEAR = 2022

# (Optionnel) dossier où sauvegarder une copie locale du parquet (debug)
SAVE_LOCAL = False
DROP_DIR = Path(os.getenv(
    "DROP_DIR",
    r"C:\Users\loudo\Documents\YNOV COURS\CERTIFICATION YNOV\med_demo\drop"
))
OUT_FILE = DROP_DIR / f"pro_sante_{YEAR}.parquet"

# URL data.ameli → export Parquet filtré sur l'année
AMELI_URL = (
    "https://data.ameli.fr/api/explore/v2.1/catalog/datasets/"
    "demographie-effectifs-et-les-densites/exports/parquet"
    f"?lang=fr&refine=annee%3A%22{YEAR}%22&timezone=Europe%2FBerlin"
)

# Connexion Postgres (à adapter ou via variables d'environnement)
PG_CONN = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE", "Health_Professional"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "Sky.tess31310"),
)

# Colonnes attendues par la staging (ordre strict)
COLS = [
    "annee", "profession_sante", "region", "libelle_region",
    "departement", "libelle_departement",
    "classe_age", "libelle_classe_age", "libelle_sexe",
    "effectif", "densite",
    "vision_generale_all", "vision_generale_prescriptions", "vision_profession_territoire"
]


def fetch_parquet_df(url: str) -> pd.DataFrame:
    """
    Lecture directe du Parquet en HTTP via pyarrow (pas d'écriture disque nécessaire).
    """
    print(f"[INFO] Lecture Parquet directe depuis l’URL…\n{url}")
    df = pd.read_parquet(url, engine="pyarrow")
    print(
        f"[OK] Parquet chargé en mémoire: {len(df):,} lignes, {len(df.columns)} colonnes")
    return df


def prepare_dataframe(df: pd.DataFrame, year: int) -> pd.DataFrame:
    for c in COLS:
        if c not in df.columns:
            df[c] = None
    df = df[COLS]

    # Harmoniser le type année (au cas où)
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

    df = df[df["annee"] == year].copy()
    df["effectif"] = pd.to_numeric(df["effectif"], errors="coerce")
    df["densite"] = pd.to_numeric(df["densite"],  errors="coerce")

    print(f"[INFO] Après préparation: {len(df):,} lignes pour l’année {year}")
    return df


def upsert_to_staging(df: pd.DataFrame, year: int):
    """
    COPY dans table temporaire -> UPSERT vers "pro_sante".stg_pro_sante_raw
    """
    if df.empty:
        print(
            f"[WARN] Aucune ligne pour l’année {year}. Abandon du chargement.")
        return

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_stg
                (LIKE "pro_sante".stg_pro_sante_raw INCLUDING DEFAULTS)
                ON COMMIT DROP;
            """)
        # TSV sans en-têtes
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="")
        buf.seek(0)

        with conn.cursor() as cur:
            cur.copy_from(buf, "tmp_stg", sep="\t", null="")
            cur.execute("""
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
            """, (year,))
        print(f"[OK] UPSERT staging terminé pour {year}")


def main():
    df = fetch_parquet_df(AMELI_URL)
    if SAVE_LOCAL:
        try:
            DROP_DIR.mkdir(parents=True, exist_ok=True)
            df.to_parquet(OUT_FILE, engine="pyarrow", index=False)
            print(f"[INFO] Copie locale écrite → {OUT_FILE}")
        except Exception as e:
            print(f"[WARN] Impossible d’écrire la copie locale: {e}")

    df_ready = prepare_dataframe(df, YEAR)
    upsert_to_staging(df_ready, YEAR)
    print("[DONE] Téléchargement direct + staging OK.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERREUR] {e}")
        sys.exit(1)
