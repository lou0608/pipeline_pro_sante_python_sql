# ops_logger.py
import os
import uuid
import json
from datetime import datetime, timezone
import psycopg2


def _conn():
    pwd = os.getenv("PGPASSWORD")
    if not pwd:
        raise RuntimeError("PGPASSWORD manquant (env/Secrets).")
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "Health_Professional"),
        user=os.getenv("PGUSER", "postgres"),
        password=pwd,
    )


def log_run(pipeline, component, started_at, finished_at, status,
            rows_written=None, message=None, extra=None):
    run_id = str(uuid.uuid4())
    duration_s = (finished_at - started_at).total_seconds()
    with _conn() as cn, cn.cursor() as cur:
        cur.execute("""
            INSERT INTO ops.run_log(run_id, pipeline, component, started_at, finished_at,
                                    duration_s, status, rows_written, message, extra)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, pipeline, component, started_at, finished_at, duration_s,
              status, rows_written, message, json.dumps(extra or {})))


def log_volume(pipeline, entity, as_of_date, volume,
               expected_min=None, expected_max=None, agg_window='Y', extra=None):
    with _conn() as cn, cn.cursor() as cur:
        cur.execute("""
            INSERT INTO ops.volumes(pipeline, entity, as_of_date, agg_window, volume, expected_min, expected_max, extra)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (pipeline, entity, as_of_date, agg_window) DO UPDATE
            SET volume = EXCLUDED.volume,
                expected_min = COALESCE(EXCLUDED.expected_min, ops.volumes.expected_min),
                expected_max = COALESCE(EXCLUDED.expected_max, ops.volumes.expected_max),
                extra = COALESCE(EXCLUDED.extra, ops.volumes.extra)
        """, (pipeline, entity, as_of_date, agg_window, volume,
              expected_min, expected_max, json.dumps(extra or {})))
