import os
import time
import glob
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from pathlib import Path


# ====== CONFIG ======
# dossier à surveiller
DROP_DIR = Path(os.getenv(
    "DROP_DIR",
    r"C:\Users\loudo\Documents\YNOV COURS\CERTIFICATION YNOV\med_demo\drop"
))
"""PG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "Health_Professional"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "Sky.tess31310"),
}"""

PG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "Health_Professional"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD"),
}
if not PG["password"]:
    raise RuntimeError("PGPASSWORD manquant (env/Secrets).")

TABLE = '"pro_sante".stg_pro_sante_raw'

# Colonnes attendues (ton schéma Parquet)
COLUMNS = [
    "annee", "profession_sante", "region", "libelle_region", "departement",
    "libelle_departement", "classe_age", "libelle_classe_age", "libelle_sexe",
    "effectif", "densite", "vision_generale_all", "vision_generale_prescriptions",
    "vision_profession_territoire"
]

PK = ["annee", "region", "departement",
      "profession_sante", "classe_age", "libelle_sexe"]

# ====== FONCTIONS ======


def read_any(path: str) -> pd.DataFrame:
    import pandas as pd
    import os

    ext = os.path.splitext(path)[1].lower()
    if ext == ".parquet":
        df = pd.read_parquet(path, engine="pyarrow")
    elif ext == ".csv":
        df = pd.read_csv(path, encoding="utf-8", sep=",")
    else:
        raise ValueError(f"Format non supporté: {ext}")

    # garder les colonnes attendues si le fichier en contient plus
    keep = [c for c in COLUMNS if c in df.columns]
    df = df[keep].copy()

    # ---- Typages / normalisation ----
    # annee peut être date -> extraire l'année
    if "annee" in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df["annee"]):
            df["annee"] = df["annee"].dt.year.astype("Int64")
        else:
            y = pd.to_datetime(df["annee"], errors="coerce").dt.year
            y = y.fillna(pd.to_numeric(df["annee"], errors="coerce"))
            df["annee"] = y.astype("Int64")

    if "effectif" in df.columns:
        df["effectif"] = pd.to_numeric(
            df["effectif"], errors="coerce").fillna(0).astype("Int64")
    if "densite" in df.columns:
        df["densite"] = pd.to_numeric(df["densite"], errors="coerce")

    # nettoyage texte (utiliser .str.strip() sur Series)
    for c in ["profession_sante", "region", "libelle_region", "departement",
              "libelle_departement", "classe_age", "libelle_classe_age", "libelle_sexe",
              "vision_generale_all", "vision_generale_prescriptions", "vision_profession_territoire"]:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # homogénéiser le sexe
    if "libelle_sexe" in df.columns:
        sex = df["libelle_sexe"].str.lower().str.strip()
        df["libelle_sexe"] = sex.map({
            "homme": "hommes", "hommes": "hommes",
            "femme": "femmes", "femmes": "femmes",
            "masculin": "hommes", "féminin": "femmes"
        }).fillna(sex)

    # ---- Lignes indispensables pour la PK ----
    df = df.dropna(subset=["annee", "region", "departement",
                   "profession_sante", "classe_age", "libelle_sexe"])

    # remplacer NaN par None pour psycopg2
    df = df.where(pd.notnull(df), None)

    print(
        f"[debug] lu {len(df)} lignes après normalisation (fichier: {os.path.basename(path)})")
    return df


def upsert_dataframe(conn, df: pd.DataFrame):
    if df.empty:
        return 0

    # Assurer que toutes les colonnes existent (mettre None si manquantes)
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[COLUMNS]

    # Construction clause INSERT VALUES %s (execute_values)
    insert_cols = ",".join(COLUMNS)
    conflict_cols = ",".join(PK)

    # on met à jour tout sauf la PK
    update_cols = [c for c in COLUMNS if c not in PK]
    set_clause = ", ".join([f'{c} = EXCLUDED.{c}' for c in update_cols])

    sql = f"""
    INSERT INTO {TABLE} ({insert_cols})
    VALUES %s
    ON CONFLICT ({conflict_cols})
    DO UPDATE SET {set_clause};
    """

    records = [tuple(None if pd.isna(v) else int(v) if isinstance(v, (pd.Int64Dtype().type,)) else v
                     for v in row)
               for row in df.itertuples(index=False, name=None)]

    with conn:
        with conn.cursor() as cur:
            extras.execute_values(cur, sql, records, page_size=10000)
    return len(records)

# ====== MAIN LOOP ======


def main():
    os.makedirs(DROP_DIR, exist_ok=True)
    print(f"[watch] Surveillance de {DROP_DIR}")
    seen = set()
    conn = psycopg2.connect(**PG)

    try:
        while True:
            paths = glob.glob(os.path.join(DROP_DIR, "*.parquet")) + \
                glob.glob(os.path.join(DROP_DIR, "*.csv"))
            for path in sorted(paths):
                if path in seen:
                    continue
                try:
                    df = read_any(path)
                    inserted = upsert_dataframe(conn, df)
                    print(
                        f"[watch] {os.path.basename(path)} → upsert {inserted} lignes")
                except Exception as e:
                    print(f"[watch][ERREUR] {os.path.basename(path)}: {e}")
                finally:
                    seen.add(path)
            time.sleep(3)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
