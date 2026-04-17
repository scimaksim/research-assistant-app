"""Create catalog (if missing), schema, volume, and metadata table.

Idempotent — safe to re-run. Uses the SQL Statement Execution API so it can run
from a serverless job without Spark.
"""
import argparse
import sys
import time

import httpx
from databricks.sdk import WorkspaceClient


def run_sql(w: WorkspaceClient, warehouse_id: str, stmt: str) -> dict:
    host = w.config.host.rstrip("/")
    auth = w.config.authenticate()
    url = f"{host}/api/2.0/sql/statements"
    body = {"warehouse_id": warehouse_id, "statement": stmt, "wait_timeout": "50s"}
    r = httpx.post(url, headers=auth, json=body, timeout=120.0)
    r.raise_for_status()
    d = r.json()
    state = d.get("status", {}).get("state")
    sid = d.get("statement_id")
    while state in ("PENDING", "RUNNING"):
        time.sleep(2)
        rr = httpx.get(f"{url}/{sid}", headers=auth, timeout=30.0)
        rr.raise_for_status()
        d = rr.json()
        state = d.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({state}): {d}")
    return d


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--volume", required=True)
    p.add_argument("--metadata-table", required=True)
    p.add_argument("--warehouse-id", required=True)
    args = p.parse_args()

    w = WorkspaceClient()
    wid = args.warehouse_id
    cat, sch, vol, tbl = args.catalog, args.schema, args.volume, args.metadata_table

    print(f"[setup_uc] catalog={cat} schema={sch} volume={vol} table={tbl}")
    run_sql(w, wid, f"CREATE CATALOG IF NOT EXISTS {cat}")
    run_sql(w, wid, f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
    run_sql(w, wid, f"CREATE VOLUME IF NOT EXISTS {cat}.{sch}.{vol}")
    run_sql(
        w,
        wid,
        f"""
        CREATE TABLE IF NOT EXISTS {cat}.{sch}.{tbl} (
          report_id          STRING,
          title              STRING,
          report_series      STRING,
          primary_author     STRING,
          team               STRING,
          asset_class        STRING,
          region             STRING,
          publication_date   STRING,
          publication_timestamp TIMESTAMP,
          summary            STRING,
          volume_path        STRING
        )
        """,
    )
    print("[setup_uc] done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[setup_uc] FAILED: {e}", file=sys.stderr)
        raise
