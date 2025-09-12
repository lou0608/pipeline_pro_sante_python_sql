# -*- coding: utf-8 -*-
# facts_loader.py
from __future__ import annotations
import os
import logging
import psycopg2
from typing import Optional

from datetime import date
from ops_logger import log_volume

# ==== LOGGING ====
log = logging.getLogger("facts_loader")

# ==== CONFIG CONNEXION ====
PG_CONN = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "Health_Professional"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD"),  # pas de défaut
}
if not PG_CONN["password"]:
    raise RuntimeError("PGPASSWORD manquant (env/Secrets).")


def _exec_sql(conn, sql: str, params: tuple, label: str) -> int:
    with conn.cursor() as cur:
        log.info("SQL → %s", label)
        cur.execute(sql, params)
        # rowcount sur INSERT..ON CONFLICT peut être -1 selon drivers,
        # on journalise quand même.
        try:
            log.info("→ %s lignes affectées", cur.rowcount)
        except Exception:
            pass
    return 0


def load_facts(year: Optional[int]) -> None:
    """
    Alimente "pro_sante".table_faits_pro_v2 à partir de la staging.
    Si year est None → charge toutes les années présentes en staging.
    Requiert les dimensions déjà peuplées.
    """
    year_filter = "AND annee = %s" if year is not None else ""
    params = (year,) if year is not None else tuple()

    sql = f"""
        WITH s AS (
          SELECT *
          FROM "pro_sante".stg_pro_sante_raw
          WHERE region <> '99'
            AND departement <> '999'
            AND libelle_sexe IN ('hommes','femmes')
            AND classe_age <> 'tout_age'
            {year_filter}
        ),
        j AS (
          SELECT
            a.id_annee,
            p.id_profession,
            r.id_region,
            /* Mapping Corse 2A/2B → 101/102 avant jointure */
            d.id_departement,
            t.id_tranche,
            g.id_genre,
            s.effectif::int                AS effectif,
            s.densite::double precision    AS densite
          FROM (
            SELECT
              s.*,
              CASE
                WHEN s.departement = '2A' THEN 101
                WHEN s.departement = '2B' THEN 102
                ELSE NULLIF(s.departement, '')::int
              END AS dep_norm
            FROM s
          ) s
          JOIN "pro_sante".annees      a ON a.annee          = s.annee
          JOIN "pro_sante".professions p ON p.profession     = s.profession_sante
          JOIN "pro_sante".regions     r ON r.libelle_region = s.libelle_region
          JOIN "pro_sante".departements d ON d.id_departement = s.dep_norm
          JOIN "pro_sante".tranches_age t ON t.classe_age    = s.classe_age
          JOIN "pro_sante".genres       g ON g.libelle_sexe  = s.libelle_sexe
        )
        INSERT INTO "pro_sante".table_faits_pro_v2
          (id_annee, id_profession, id_region, id_departement, id_tranche, id_genre, effectif, densite)
        SELECT
          id_annee, id_profession, id_region, id_departement, id_tranche, id_genre, effectif, densite
        FROM j
        ON CONFLICT (id_annee, id_profession, id_region, id_departement, id_tranche, id_genre)
        DO UPDATE SET
          effectif = EXCLUDED.effectif,
          densite  = EXCLUDED.densite;
    """

    with psycopg2.connect(**PG_CONN) as conn:
        _exec_sql(conn, sql, params, "FACTS upsert")
        conn.commit()
        log.info("[OK] Faits chargés%s", f" pour {year}" if year else "")
        # Compte des lignes de faits pour l'année y
    if year is not None:
        with psycopg2.connect(**PG_CONN) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM "pro_sante".table_faits_pro_v2 f
                WHERE f.id_annee = (SELECT id_annee FROM "pro_sante".annees WHERE annee = %s)
            """, (year,))
            (cnt_faits,) = cur.fetchone()

        log_volume(
            pipeline="pro_sante",
            entity="table_faits_pro_v2",
            as_of_date=date(year, 12, 31),
            volume=cnt_faits,
            expected_min=1,   # seuil initial simple
            agg_window='Y',
            extra={"year": year}
        )


# ==== MAIN ====


def main(year: Optional[int] = None):
    """
    Point d'entrée. Résout l'année au runtime :
    - priorité à l'argument,
    - sinon ENV YEAR,
    - sinon charge tout (None).
    """
    y_env = os.getenv("YEAR")
    y = year if year is not None else (int(y_env) if y_env else None)
    load_facts(y)


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, os.getenv(
            "LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
