# dims_loader.py
from __future__ import annotations
import logging
import psycopg2

# ==== CONFIG CONNEXION ====
PG_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "Health_Professional",
    "user": "postgres",
    "password": "Sky.tess31310"   # ⚠️ adapte avec ton mot de passe
}

YEAR = None  # Mets une année (ex: 2023) ou None pour tout charger


# ==== LOGGER ====
log = logging.getLogger("dims_loader")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"))
log.addHandler(handler)


def _exec_sql(conn, sql: str, params=None, label: str = ""):
    with conn.cursor() as cur:
        log.info("SQL → %s", label or sql.splitlines()[0][:80] + "…")
        cur.execute(sql, params or ())
        try:
            rows = cur.rowcount
            if rows is not None and rows >= 0:
                log.info("→ %s lignes affectées", rows)
        except Exception:
            pass


def populate_dimensions(year: int | None = None):
    """
    Étape 3 — Remplit/actualise les dimensions depuis la staging.
    Si 'year' est fourni, filtre la staging sur cette année.
    """
    year_filter = "AND annee = %s" if year is not None else ""
    params = (year,) if year is not None else tuple()

    with psycopg2.connect(**PG_CONN) as conn:
        # Années
        _exec_sql(conn, f"""
            WITH s AS (
              SELECT * FROM "pro_sante".stg_pro_sante_raw
              WHERE region <> '99'
                AND departement <> '999'
                AND libelle_sexe IN ('hommes','femmes')
                AND classe_age <> 'tout_age'
                {year_filter}
            )
            INSERT INTO "pro_sante".annees (annee)
            SELECT DISTINCT annee FROM s
            ON CONFLICT (annee) DO NOTHING;
        """, params, "DIM annee")

        # Professions
        _exec_sql(conn, f"""
            WITH s AS (
              SELECT * FROM "pro_sante".stg_pro_sante_raw
              WHERE region <> '99'
                AND departement <> '999'
                AND libelle_sexe IN ('hommes','femmes')
                AND classe_age <> 'tout_age'
                {year_filter}
            )
            INSERT INTO "pro_sante".professions (profession)
            SELECT DISTINCT profession_sante FROM s
            ON CONFLICT (profession) DO NOTHING;
        """, params, "DIM profession")

        # Régions
        _exec_sql(conn, f"""
            WITH s AS (
             SELECT * FROM "pro_sante".stg_pro_sante_raw
            WHERE region <> '99'
                AND departement <> '999'
                AND libelle_sexe IN ('hommes','femmes')
                AND classe_age <> 'tout_age'
                {year_filter}
    )
        INSERT INTO "pro_sante".regions (libelle_region)
        SELECT DISTINCT libelle_region FROM s
        ON CONFLICT (libelle_region) DO NOTHING;
        """, params, "DIM region")

        # Genres
        _exec_sql(conn, f"""
            WITH s AS (
              SELECT * FROM "pro_sante".stg_pro_sante_raw
              WHERE region <> '99'
                AND departement <> '999'
                AND libelle_sexe IN ('hommes','femmes')
                AND classe_age <> 'tout_age'
                {year_filter}
            )
            INSERT INTO "pro_sante".genres (libelle_sexe)
            SELECT DISTINCT libelle_sexe FROM s
            ON CONFLICT (libelle_sexe) DO NOTHING;
        """, params, "DIM genre")

        # Tranches d'âge
        _exec_sql(conn, f"""
            WITH s AS (
              SELECT * FROM "pro_sante".stg_pro_sante_raw
              WHERE region <> '99'
                AND departement <> '999'
                AND libelle_sexe IN ('hommes','femmes')
                AND classe_age <> 'tout_age'
                {year_filter}
            )
            INSERT INTO "pro_sante".tranches_age (classe_age, libelle_classe_age)
            SELECT DISTINCT classe_age, libelle_classe_age FROM s
            ON CONFLICT (classe_age) DO UPDATE
            SET libelle_classe_age = EXCLUDED.libelle_classe_age;
        """, params, "DIM tranche_age")

        # Départements (avec fk région)
        _exec_sql(conn, f"""
            WITH s AS (
                SELECT *
                FROM "pro_sante".stg_pro_sante_raw
                WHERE region <> '99'
                AND departement <> '999'
                AND libelle_sexe IN ('hommes','femmes')
                AND classe_age <> 'tout_age'
                {year_filter}
    ),
            n AS (
            SELECT
                CASE
                WHEN departement = '2A' THEN 101
                WHEN departement = '2B' THEN 102
                WHEN departement ~ '^[0-9]+$' THEN departement::int
                ELSE NULL
                END AS id_departement,
                libelle_departement AS nom_departement,
                libelle_region
                FROM s
    )
        INSERT INTO "pro_sante".departements (id_departement, nom_departement, id_region)
        SELECT DISTINCT
          n.id_departement,
          n.nom_departement,
          r.id_region
        FROM n
        JOIN "pro_sante".regions r ON r.libelle_region = n.libelle_region
        WHERE n.id_departement IS NOT NULL
        ON CONFLICT (id_departement) DO UPDATE
        SET nom_departement = EXCLUDED.nom_departement,
        id_region       = EXCLUDED.id_region;
""", params, "DIM departement")

        conn.commit()
        log.info("[OK] Dimensions peuplées%s", f" pour {year}" if year else "")


# ==== MAIN ====
def main():
    populate_dimensions(YEAR)


if __name__ == "__main__":
    main()
